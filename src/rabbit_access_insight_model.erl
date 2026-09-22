%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 martinx
%% SPDX-License-Identifier: MPL-2.0
%%
%% How journal records turn into aggregates.
%%
%% apply/3 is the only function that changes rows of the local origin, and
%% it is used both live and when replaying the journal after a restart, so
%% the aggregates are always a function of the records. Every row carries
%% `v`, the sequence number of the last record that changed it; the origin
%% row carries the highest sequence number applied. Replication relies on
%% both (see rabbit_access_insight_sync).
-module(rabbit_access_insight_model).

-include("rabbit_access_insight.hrl").

-export([create_tables/0, apply/3, remember/1, new_origin/2, origin_version/1,
         merge_user/2, merge_counts/2, prune_daily/2]).

-import(rabbit_access_insight_util, [day/1, days_between/2]).

create_tables() ->
    [ets:new(T, [named_table, public, set, {read_concurrency, true}])
     || T <- ?AGG_TABLES ++ ?LOCAL_TABLES, T =/= ?T_RECENT],
    ets:new(?T_RECENT, [named_table, public, ordered_set, {read_concurrency, true}]),
    ok.

new_origin(Origin, Now) ->
    case ets:lookup(?T_ORIGIN, Origin) of
        [] -> ets:insert(?T_ORIGIN, {Origin, #{v => 0, created_at => Now,
                                               updated_at => Now, gaps => []}});
        _  -> true
    end,
    ok.

origin_version(Origin) ->
    case ets:lookup(?T_ORIGIN, Origin) of
        [{_, #{v := V}}] -> V;
        []               -> 0
    end.

%%----------------------------------------------------------------------------

-spec apply(term(), tuple(), map()) -> ok.
apply(Origin, ?REC(Seq, Ts, session_open, M), Limits) ->
    #{user := User, vhost := VHost} = M,
    Method = maps:get(method, M, <<"unknown">>),
    Key = user_key(Origin, User, Limits),
    update(?T_USER, Key, Seq, fun(U) ->
        U#{sessions   => maps:get(sessions, U, 0) + 1,
           first_seen => min_ts(maps:get(first_seen, U, undefined), Ts),
           last_seen  => max(maps:get(last_seen, U, 0), Ts),
           methods    => inc(maps:get(methods, U, #{}), Method, Limits),
           method_sources => inc(maps:get(method_sources, U, #{}),
                                 maps:get(method_source, M, <<"none">>), Limits),
           sources    => inc(maps:get(sources, U, #{}), maps:get(peer, M, <<"unknown">>), Limits),
           clients    => inc(maps:get(clients, U, #{}), maps:get(client, M, <<"unknown">>), Limits),
           vhosts     => inc(maps:get(vhosts, U, #{}), VHost, Limits),
           protocols  => inc(maps:get(protocols, U, #{}), maps:get(protocol, M, <<"unknown">>), Limits)}
    end),
    update(?T_DAILY, {Origin, element(2, Key), VHost, day(Ts)}, Seq,
           fun(D) -> D#{sessions => maps:get(sessions, D, 0) + 1} end),
    touch(Origin, Seq, Ts);

apply(Origin, ?REC(Seq, Ts, session_close, M), Limits) ->
    #{user := User, vhost := VHost, opened_at := Opened} = M,
    Key = user_key(Origin, User, Limits),
    Start = max(Opened, maps:get(counted_from, M, Opened)),
    Split = days_between(Start, Ts),
    Total = lists:sum([Ms || {_, Ms} <- Split]),
    update(?T_USER, Key, Seq, fun(U) ->
        U#{online_ms => maps:get(online_ms, U, 0) + Total,
           last_seen => max(maps:get(last_seen, U, 0), Ts)}
    end),
    [update(?T_DAILY, {Origin, element(2, Key), VHost, Day}, Seq,
            fun(D) -> D#{online_ms => maps:get(online_ms, D, 0) + Ms} end)
     || {Day, Ms} <- Split],
    touch(Origin, Seq, Ts);

apply(Origin, ?REC(Seq, Ts, login_failed, M), Limits) ->
    #{user := User, stage := Stage} = M,
    Key = user_key(Origin, User, Limits),
    Field = case Stage of credentials -> failed; _ -> refused end,
    update(?T_USER, Key, Seq, fun(U) ->
        U#{Field       => maps:get(Field, U, 0) + 1,
           last_failed => max(maps:get(last_failed, U, 0), Ts)}
    end),
    update(?T_DAILY, {Origin, element(2, Key), maps:get(vhost, M, <<>>), day(Ts)}, Seq,
           fun(D) -> D#{Field => maps:get(Field, D, 0) + 1} end),
    Source = maps:get(peer, M, <<"unknown">>),
    Reason = maps:get(reason, M, <<>>),
    update(?T_FAIL, {Origin, element(2, Key), Source, Stage, Reason}, Seq,
           fun(F) -> F#{count => maps:get(count, F, 0) + 1,
                        first => min_ts(maps:get(first, F, undefined), Ts),
                        last  => max(maps:get(last, F, 0), Ts)} end),
    touch(Origin, Seq, Ts);

apply(Origin, ?REC(Seq, Ts, gap, M), _Limits) ->
    [{_, O}] = ets:lookup(?T_ORIGIN, Origin),
    Gaps = lists:sublist([M#{at => Ts} | maps:get(gaps, O, [])], 200),
    ets:insert(?T_ORIGIN, {Origin, O#{gaps => Gaps}}),
    touch(Origin, Seq, Ts);

apply(Origin, ?REC(Seq, Ts, _Other, _M), _Limits) ->
    touch(Origin, Seq, Ts).

%% Keep the last closed sessions and failed logins of this node in memory,
%% for recent-activity views that should not scan the journal.
remember(R = ?REC(Seq, _, Type, _)) when Type =:= session_close; Type =:= login_failed ->
    ets:insert(?T_RECENT, {Seq, R}),
    case ets:info(?T_RECENT, size) > ?RECENT_MAX of
        true  -> ets:delete(?T_RECENT, ets:first(?T_RECENT));
        false -> true
    end,
    ok;
remember(_) -> ok.

%%----------------------------------------------------------------------------

%% A user seen for the first time once max_users rows exist for this origin
%% is counted under "(other)", so random user names cannot grow the tables
%% without bound.
user_key(Origin, User, #{max_users := Max}) ->
    Key = {Origin, User},
    case ets:member(?T_USER, Key) of
        true -> Key;
        false ->
            [{_, O}] = ets:lookup(?T_ORIGIN, Origin),
            case maps:get(users, O, 0) < Max of
                true  -> ets:insert(?T_ORIGIN, {Origin, O#{users => maps:get(users, O, 0) + 1}}),
                         Key;
                false -> {Origin, ?OTHER}
            end
    end.

update(Tab, Key, Seq, Fun) ->
    Old = case ets:lookup(Tab, Key) of
              [{_, V}] -> V;
              []       -> #{}
          end,
    ets:insert(Tab, {Key, (Fun(Old))#{v => Seq}}).

touch(Origin, Seq, Ts) ->
    [{_, O}] = ets:lookup(?T_ORIGIN, Origin),
    ets:insert(?T_ORIGIN, {Origin, O#{v => max(Seq, maps:get(v, O, 0)),
                                      updated_at => max(Ts, maps:get(updated_at, O, 0))}}),
    ok.

%% Count Key in a bounded map; keys beyond the limit are counted as "(other)".
inc(Map, Key, #{max_set := Max}) ->
    case maps:is_key(Key, Map) orelse map_size(Map) < Max of
        true  -> Map#{Key => maps:get(Key, Map, 0) + 1};
        false -> Map#{?OTHER => maps:get(?OTHER, Map, 0) + 1}
    end.

min_ts(undefined, Ts) -> Ts;
min_ts(A, B) -> min(A, B).

%%----------------------------------------------------------------------------
%% Merging rows of different origins for the cluster view: counts add up,
%% first seen is the earliest, last seen the latest, sets are united.

-spec merge_user(map(), map()) -> map().
merge_user(A, B) ->
    maps:fold(fun(K, VB, Acc) ->
        case maps:find(K, Acc) of
            error    -> Acc#{K => VB};
            {ok, VA} -> Acc#{K => merge_field(K, VA, VB)}
        end
    end, A, B).

merge_field(first_seen, A, B) -> min(A, B);
merge_field(first, A, B)      -> min(A, B);
merge_field(last_seen, A, B)  -> max(A, B);
merge_field(last_failed, A, B)-> max(A, B);
merge_field(last, A, B)       -> max(A, B);
merge_field(v, A, B)          -> max(A, B);
merge_field(_, A, B) when is_integer(A), is_integer(B) -> A + B;
merge_field(_, A, B) when is_map(A), is_map(B) -> merge_counts(A, B);
merge_field(_, _A, B) -> B.

merge_counts(A, B) ->
    maps:fold(fun(K, V, Acc) -> Acc#{K => maps:get(K, Acc, 0) + V} end, A, B).

%% Drop daily rows older than the retention, for every origin. The rule
%% depends only on the date, so every node prunes replicas the same way and
%% no deletion has to be replicated.
prune_daily(Now, Days) ->
    Cutoff = day(Now - Days * 86400000),
    ets:select_delete(?T_DAILY, [{{{'_', '_', '_', '$1'}, '_'}, [{'<', '$1', Cutoff}], [true]}]).
