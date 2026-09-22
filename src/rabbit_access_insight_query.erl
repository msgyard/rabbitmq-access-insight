%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 martinx
%% SPDX-License-Identifier: MPL-2.0
%%
%% The cluster view, answered from any node.
%%
%% Aggregates come from this node's tables, which hold its own contribution
%% and replicas of every other node's; a query first pulls anything newer from
%% the running peers (at most every five seconds), then merges one copy per
%% origin. Live data -- open sessions, recent activity, status -- is asked of
%% every running node directly; a node that does not answer is listed in
%% `incomplete`.
-module(rabbit_access_insight_query).

-include("rabbit_access_insight.hrl").

-export([overview/0, users/1, user/1, sessions/1, auth/1, nodes/0,
         records/3, capabilities/0, report/0, forget_node/1]).
%% called on every node
-export([local_live/0, local_capabilities/0, local_records/2]).

-import(rabbit_access_insight_util, [now_ms/0]).

-define(RPC_TIMEOUT, 8000).
-define(DAY, 86400000).

%%----------------------------------------------------------------------------

overview() ->
    fresh(),
    Now = now_ms(),
    {Live, Incomplete} = live(),
    Users = user_rows(Now, Live),
    Today = rabbit_access_insight_util:day(Now),
    DailyToday = daily_totals(fun(Day) -> Day =:= Today end),
    States = count_by(fun(#{state := St}) -> St end, Users),
    Methods = lists:foldl(fun(U, Acc) -> rabbit_access_insight_model:merge_counts(Acc, maps:get(methods, U, #{})) end,
                          #{}, Users),
    Sources = lists:foldl(fun(U, Acc) -> rabbit_access_insight_model:merge_counts(Acc, maps:get(method_sources, U, #{})) end,
                          #{}, Users),
    #{generated_at => Now,
      users => #{total => length(Users),
                 defined => length([x || #{defined := true} <- Users]),
                 by_state => States,
                 in_use_undefined => length([x || #{defined := false, sessions := N} <- Users, N > 0]),
                 never_used => maps:get(never_used, States, 0)},
      sessions => #{open => length(open_sessions(Live)),
                    today => maps:get(sessions, DailyToday, 0)},
      logins_today => #{succeeded => maps:get(sessions, DailyToday, 0),
                        failed => maps:get(failed, DailyToday, 0),
                        refused => maps:get(refused, DailyToday, 0)},
      methods => Methods,
      method_sources => Sources,
      nodes => node_summaries(Live),
      incomplete => Incomplete}.

users(Opts) ->
    fresh(),
    Now = now_ms(),
    {Live, Incomplete} = live(),
    Rows0 = user_rows(Now, Live),
    Rows1 = case maps:get(state, Opts, undefined) of
                undefined -> Rows0;
                St -> [R || R = #{state := S} <- Rows0, atom_to_binary(S, utf8) =:= St]
            end,
    Rows2 = case maps:get(search, Opts, undefined) of
                undefined -> Rows1;
                <<>> -> Rows1;
                Q -> [R || R = #{name := N} <- Rows1, binary:match(string:lowercase(N), string:lowercase(Q)) =/= nomatch]
            end,
    Sort = maps:get(sort, Opts, <<"name">>),
    Rows3 = sort_rows(Rows2, Sort, maps:get(order, Opts, <<"asc">>)),
    {Page, PageSize} = {max(1, maps:get(page, Opts, 1)), min(1000, max(1, maps:get(page_size, Opts, 100)))},
    Items = lists:sublist(Rows3, (Page - 1) * PageSize + 1, PageSize),
    #{items => [maps:without([sources_map, clients_map], I) || I <- Items],
      total => length(Rows3), page => Page, page_size => PageSize,
      generated_at => Now, incomplete => Incomplete}.

user(Name) ->
    fresh(),
    Now = now_ms(),
    {Live, Incomplete} = live(),
    Merged = merged_user(Name),
    Defined = defined_users(),
    case {Merged, maps:find(Name, Defined)} of
        {undefined, error} -> not_found;
        {_, Def} ->
            Row = user_row(Name, or_empty(Merged), def(Def), Now, Live),
            Days = rabbit_access_insight_config:get(history_rollup_days),
            #{user => Row#{sources => top(maps:get(sources_map, Row), 50),
                           clients => top(maps:get(clients_map, Row), 50),
                           vhosts => maps:get(vhosts, or_empty(Merged), #{}),
                           protocols => maps:get(protocols, or_empty(Merged), #{})},
              daily => daily_series(Name, Now, min(Days, 30)),
              failures => failures(fun(U) -> U =:= Name end, 100),
              open_sessions => [S || S = #{user := U} <- open_sessions(Live), U =:= Name],
              recent => lists:sublist([R || R = #{user := U} <- recent(Live), U =:= Name], 100),
              generated_at => Now, incomplete => Incomplete}
    end.

sessions(Opts) ->
    {Live, Incomplete} = live(),
    User = maps:get(user, Opts, undefined),
    Keep = fun(#{user := U}) -> User =:= undefined orelse U =:= User end,
    Limit = min(5000, maps:get(limit, Opts, 500)),
    #{open => lists:sublist(lists:filter(Keep, open_sessions(Live)), Limit),
      recent => lists:sublist([R || R = #{type := session_close} <- recent(Live), Keep(R)], Limit),
      generated_at => now_ms(), incomplete => Incomplete}.

auth(Opts) ->
    fresh(),
    Now = now_ms(),
    {Live, Incomplete} = live(),
    Days = min(rabbit_access_insight_config:get(history_rollup_days), maps:get(days, Opts, 30)),
    Series = [#{day => D, succeeded => maps:get(sessions, T, 0), failed => maps:get(failed, T, 0),
                refused => maps:get(refused, T, 0)}
              || {D, T} <- day_range(Now, Days, fun(Day) -> daily_totals(fun(X) -> X =:= Day end) end)],
    Users = user_rows(Now, Live),
    #{series => Series,
      methods => lists:foldl(fun(U, A) -> rabbit_access_insight_model:merge_counts(A, maps:get(methods, U, #{})) end, #{}, Users),
      method_sources => lists:foldl(fun(U, A) -> rabbit_access_insight_model:merge_counts(A, maps:get(method_sources, U, #{})) end, #{}, Users),
      failures => failures(fun(_) -> true end, 200),
      recent_failures => lists:sublist([R || R = #{type := login_failed} <- recent(Live)], 200),
      generated_at => Now, incomplete => Incomplete}.

nodes() ->
    fresh(),
    {Live, Incomplete} = live(),
    #{nodes => node_summaries(Live), origins => origins(Live), incomplete => Incomplete,
      replication => catch_map(fun rabbit_access_insight_sync:status/0)}.

capabilities() ->
    Members = members(),
    {Res, _} = multicall(Members, local_capabilities, []),
    #{nodes => [case R of
                    {ok, C} -> C#{node => N, plugin => true};
                    _ -> #{node => N, plugin => false}
                end || {N, R} <- Res]}.

records(Node, Since, Limit) ->
    case erpc_call(Node, local_records, [Since, Limit]) of
        {ok, R} -> R;
        {error, E} -> #{error => E}
    end.

forget_node(Node) ->
    rabbit_access_insight_sync:forget_node(Node).

%% Account reconciliation: every defined account and every account that
%% logged in or tried to.
report() ->
    fresh(),
    Now = now_ms(),
    {Live, _} = live(),
    [maps:with([name, defined, tags, has_password, state, connected, sessions, first_seen,
                last_seen, failed, refused, methods, method_sources, sources], R#{sources => top(maps:get(sources_map, R), 5)})
     || R <- sort_rows(user_rows(Now, Live), <<"name">>, <<"asc">>)].

%%----------------------------------------------------------------------------
%% Local (per node) parts

local_live() ->
    Sessions = [S#{node => node()} || {_, S0} <- ets:tab2list(?T_SESSION),
                                      S <- [maps:without([vm], S0)]],
    Recent = [R || {_, R} <- ets:tab2list(?T_RECENT)],
    Status = rabbit_access_insight_collector:local_status(),
    #{sessions => Sessions, recent => Recent, status => Status}.

local_capabilities() ->
    Running = [A || {A, _, _} <- application:which_applications()],
    Has = fun(A) -> lists:member(A, Running) end,
    Seen = rabbit_access_insight_collector:counts(),
    #{plugin_version => plugin_version(),
      spec_version => ?SPEC_VERSION,
      rabbitmq_version => app_version(rabbit),
      management => Has(rabbitmq_management),
      management_agent => Has(rabbitmq_management_agent),
      prometheus => Has(rabbitmq_prometheus),
      event_exchange => Has(rabbitmq_event_exchange),
      collect_statistics => application:get_env(rabbit, collect_statistics, none),
      auth_backends => [backend_name(B) || B <- application:get_env(rabbit, auth_backends, [])],
      only_internal_backend => rabbit_access_insight_auth_method:only_internal(
                                 application:get_env(rabbit, auth_backends, [rabbit_auth_backend_internal])),
      access_events_seen => proplists:get_value(access_auth_verified, Seen, 0),
      metrics_export => rabbit_access_insight_metrics:export_mode(),
      http => rabbit_access_insight_http:listener_info()}.

local_records(Since, Limit) ->
    {_, Epoch} = rabbit_access_insight_collector:origin(),
    _ = rabbit_access_insight_collector:sync(),
    Recs = rabbit_access_insight_journal:read(Since, Limit),
    Info = rabbit_access_insight_journal:info(),
    #{node => node(), epoch => Epoch, oldest_seq => maps:get(oldest_seq, Info),
      records => [rec_json(R) || R <- Recs],
      next => case Recs of [] -> Since; _ -> element(1, lists:last(Recs)) end}.

%%----------------------------------------------------------------------------

fresh() -> rabbit_access_insight_sync:sync_now(8000).

members() ->
    try rabbit_nodes:list_running() catch _:_ -> [node()] end.

multicall(Nodes, F, A) ->
    Res = lists:zip(Nodes, erpc:multicall(Nodes, ?MODULE, F, A, ?RPC_TIMEOUT)),
    Bad = [N || {N, R} <- Res, element(1, R) =/= ok],
    {Res, Bad}.

erpc_call(Node, F, A) ->
    try {ok, erpc:call(Node, ?MODULE, F, A, ?RPC_TIMEOUT)}
    catch C:E -> {error, iolist_to_binary(io_lib:format("~tp:~tp", [C, E]))}
    end.

%% {[{Node, LiveMap}], IncompleteNodes}
live() ->
    {Res, Bad} = multicall(members(), local_live, []),
    {[{N, L} || {N, {ok, L}} <- Res], Bad}.

%% Open sessions of all nodes, newest first.
open_sessions(Live) ->
    [S || {_, S} <- lists:reverse(lists:keysort(1, [{maps:get(opened_at, S), S}
                                                    || {_, #{sessions := Ss}} <- Live, S <- Ss]))].

%% Recently closed sessions and failed logins of all nodes, newest first.
recent(Live) ->
    Recs = [{Ts, (maps:without([vm], M))#{type => T, at => Ts, node => N}}
            || {N, #{recent := Rs}} <- Live, ?REC(_, Ts, T, M) <- Rs],
    [R || {_, R} <- lists:reverse(lists:keysort(1, Recs))].

merged_user(Name) ->
    case ets:select(?T_USER, [{{{'_', Name}, '$1'}, [], ['$1']}]) of
        [] -> undefined;
        [H | T] -> maps:without([v], lists:foldl(fun(M, A) -> rabbit_access_insight_model:merge_user(A, M) end, H, T))
    end.

merged_users() ->
    ets:foldl(fun({{_, U}, M}, Acc) ->
                  maps:update_with(U, fun(A) -> rabbit_access_insight_model:merge_user(A, M) end, M, Acc)
              end, #{}, ?T_USER).

defined_users() ->
    try rabbit_auth_backend_internal:list_users() of
        L -> maps:from_list([{proplists:get_value(user, P),
                              #{tags => [rabbit_access_insight_util:bin(T) || T <- proplists:get_value(tags, P, [])],
                                has_password => has_password(proplists:get_value(user, P))}}
                             || P <- L])
    catch _:_ -> #{}
    end.

has_password(User) ->
    try rabbit_auth_backend_internal:lookup_user(User) of
        {ok, U} -> not lists:member(internal_user:get_password_hash(U), [<<>>, undefined]);
        _ -> false
    catch _:_ -> false
    end.

user_rows(Now, Live) ->
    Merged = merged_users(),
    Defined = defined_users(),
    Names = lists:usort(maps:keys(Merged) ++ maps:keys(Defined)),
    [user_row(N, maps:get(N, Merged, #{}), maps:get(N, Defined, undefined), Now, Live) || N <- Names].

user_row(Name, M, Def, Now, Live) ->
    Connected = length([x || {_, #{sessions := Ss}} <- Live, #{user := U} <- Ss, U =:= Name]),
    Sessions = maps:get(sessions, M, 0),
    Last = maps:get(last_seen, M, undefined),
    State = if
                Connected > 0 -> active_24h;
                Sessions =:= 0, Def =/= undefined -> never_used;
                Sessions =:= 0 -> attempts_only;
                Now - Last =< ?DAY -> active_24h;
                Now - Last =< 7 * ?DAY -> active_7d;
                true -> dormant
            end,
    #{name => Name,
      defined => Def =/= undefined,
      tags => case Def of undefined -> []; #{tags := T} -> T end,
      has_password => case Def of undefined -> false; #{has_password := P} -> P end,
      state => State,
      connected => Connected,
      sessions => Sessions,
      online_ms => maps:get(online_ms, M, 0),
      failed => maps:get(failed, M, 0),
      refused => maps:get(refused, M, 0),
      first_seen => maps:get(first_seen, M, null),
      last_seen => maps:get(last_seen, M, null),
      last_failed => maps:get(last_failed, M, null),
      methods => maps:get(methods, M, #{}),
      method_sources => maps:get(method_sources, M, #{}),
      source_count => map_size(maps:get(sources, M, #{})),
      client_count => map_size(maps:get(clients, M, #{})),
      vhost_count => map_size(maps:get(vhosts, M, #{})),
      sources_map => maps:get(sources, M, #{}),
      clients_map => maps:get(clients, M, #{})}.

or_empty(undefined) -> #{};
or_empty(M) -> M.

def({ok, D}) -> D;
def(error) -> undefined.

sort_rows(Rows, Field0, Order) ->
    Field = case Field0 of
                <<"last_seen">> -> last_seen; <<"sessions">> -> sessions; <<"failed">> -> failed;
                <<"online">> -> online_ms; <<"first_seen">> -> first_seen; <<"state">> -> state;
                _ -> name
            end,
    Key = fun(R) -> case maps:get(Field, R) of null -> 0; V -> V end end,
    Sorted = lists:sort(fun(A, B) -> {Key(A), maps:get(name, A)} =< {Key(B), maps:get(name, B)} end, Rows),
    case Order of <<"desc">> -> lists:reverse(Sorted); _ -> Sorted end.

top(Map, N) ->
    [#{name => K, count => V} || {K, V} <- lists:sublist(lists:reverse(lists:keysort(2, maps:to_list(Map))), N)].

count_by(F, L) ->
    lists:foldl(fun(X, A) -> K = F(X), A#{K => maps:get(K, A, 0) + 1} end, #{}, L).

%% Totals over daily rows whose day satisfies Pred, all users and origins.
daily_totals(Pred) ->
    ets:foldl(fun({{_, _, _, Day}, M}, Acc) ->
                  case Pred(Day) of
                      true -> rabbit_access_insight_model:merge_counts(Acc, maps:without([v], M));
                      false -> Acc
                  end
              end, #{}, ?T_DAILY).

daily_series(Name, Now, Days) ->
    Rows = ets:select(?T_DAILY, [{{{'_', Name, '_', '$1'}, '$2'}, [], [{{'$1', '$2'}}]}]),
    ByDay = lists:foldl(fun({D, M}, A) ->
                            maps:update_with(D, fun(X) -> rabbit_access_insight_model:merge_counts(X, maps:without([v], M)) end,
                                             maps:without([v], M), A)
                        end, #{}, Rows),
    [#{day => D, sessions => maps:get(sessions, T, 0), online_ms => maps:get(online_ms, T, 0),
       failed => maps:get(failed, T, 0), refused => maps:get(refused, T, 0)}
     || {D, T} <- day_range(Now, Days, fun(Day) -> maps:get(Day, ByDay, #{}) end)].

day_range(Now, Days, F) ->
    [{D, F(D)} || I <- lists:seq(Days - 1, 0, -1), D <- [rabbit_access_insight_util:day(Now - I * ?DAY)]].

failures(UserPred, Limit) ->
    Merged = ets:foldl(fun({{_, U, Src, Stage, Reason}, M}, Acc) ->
                           case UserPred(U) of
                               false -> Acc;
                               true ->
                                   K = {U, Src, Stage, Reason},
                                   maps:update_with(K, fun(A) -> rabbit_access_insight_model:merge_user(A, M) end, M, Acc)
                           end
                       end, #{}, ?T_FAIL),
    Rows = [#{user => U, source => Src, stage => Stage, reason => Reason,
              count => maps:get(count, M), first => maps:get(first, M), last => maps:get(last, M)}
            || {{U, Src, Stage, Reason}, M} <- maps:to_list(Merged)],
    lists:sublist(lists:reverse(lists:sort(fun(A, B) -> maps:get(count, A) =< maps:get(count, B) end, Rows)), Limit).

node_summaries(Live) ->
    [#{node => N, status => maps:without([journal], St), journal => maps:get(journal, St, #{})}
     || {N, #{status := St}} <- Live].

origins(Live) ->
    Running = [N || {N, _} <- Live],
    Members = try rabbit_nodes:list_members() catch _:_ -> Running end,
    Current = [{N, E} || {N, #{status := #{epoch := E}}} <- Live],
    [#{node => N, epoch => E, version => maps:get(v, M, 0), updated_at => maps:get(updated_at, M, 0),
       users => maps:get(users, M, 0), gaps => maps:get(gaps, M, []),
       state => case {lists:member({N, E}, Current), lists:member(N, Running), lists:member(N, Members)} of
                    {true, _, _} -> current;
                    {false, true, _} -> previous_epoch;
                    {false, false, true} -> not_running;
                    {false, false, false} -> left_cluster
                end}
     || {{N, E}, M} <- lists:sort(ets:tab2list(?T_ORIGIN))].

rec_json(?REC(Seq, Ts, Type, M)) ->
    #{seq => Seq, ts => Ts, type => Type, data => maps:without([vm], M)}.

plugin_version() -> app_version(?APP).

app_version(App) ->
    case application:get_key(App, vsn) of
        {ok, V} -> list_to_binary(V);
        _ -> <<"unknown">>
    end.

backend_name({A, B}) -> #{authn => A, authz => B};
backend_name(A) -> A.

catch_map(F) -> try F() catch _:_ -> #{} end.
