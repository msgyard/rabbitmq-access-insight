%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 martinx
%% SPDX-License-Identifier: MPL-2.0
%%
%% Access metrics of this node, in a neutral form with two renderers: a
%% Prometheus collector registered with rabbitmq_prometheus (so the metrics
%% appear on :15692/metrics), and plain text exposition for the plugin's own
%% endpoint when rabbitmq_prometheus is not enabled.
%%
%% Like RabbitMQ's own metrics, each node reports what happened on it; sum
%% across nodes in PromQL. rabbitmq_access_users is the exception: it is the
%% cluster view, the same on every node (use max()).
-module(rabbit_access_insight_metrics).
-behaviour(gen_server).

-include("rabbit_access_insight.hrl").

-export([start_link/0, metrics/0, render_text/1, export_mode/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(COLLECTOR, rabbit_access_insight_prometheus).
-define(CHECK_MS, 15000).
-define(BUCKETS, [1, 10, 60, 300, 1800, 3600, 21600, 86400]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% prometheus | standalone | both | off, after resolving auto.
export_mode() ->
    case rabbit_access_insight_config:get(metrics_export) of
        auto -> case prometheus_running() of true -> prometheus; false -> standalone end;
        M -> M
    end.

%% [{Name, Type, Help, Samples}]
%%   gauge / counter samples: {Labels, Value}
%%   histogram samples:       {Labels, [{Bound, CumulativeCount}], Count, SumSeconds}
metrics() ->
    PerUser = rabbit_access_insight_config:get(metrics_per_user),
    U = fun(L) -> case PerUser of true -> L; false -> lists:keydelete("user", 1, L) end end,
    Sessions = [S || {_, S} <- ets:tab2list(?T_SESSION)],
    Ctr = ets:tab2list(?T_CTR),
    Now = rabbit_access_insight_util:now_ms(),
    [{"rabbitmq_access_sessions_active", gauge,
      "Open sessions on this node",
      sum_by([{U([{"vhost", V}, {"user", Us}, {"auth_method", M}]), 1}
              || #{vhost := V, user := Us, method := M} <- Sessions])},
     {"rabbitmq_access_sessions_opened_total", counter,
      "Sessions opened on this node",
      sum_by([{U([{"vhost", V}, {"user", Us}, {"auth_method", M}, {"protocol", P}]), N}
              || {{opened, V, Us, M, P}, N} <- Ctr])},
     {"rabbitmq_access_auth_attempts_total", counter,
      "Logins on this node by result; stage is where a failed login was refused",
      sum_by([{U([{"user", Us}, {"auth_method", M}, {"result", R}, {"stage", St}]), N}
              || {{auth, Us, M, R, St}, N} <- Ctr])},
     {"rabbitmq_access_session_duration_seconds", histogram,
      "Duration of sessions closed on this node",
      histograms([{U([{"vhost", V}, {"user", Us}]), H} || {{duration, V, Us}, H} <- Ctr])},
     {"rabbitmq_access_token_expiry_seconds", gauge,
      "Seconds until the earliest token of an open session expires, when the backend reports exp",
      min_by([{U([{"user", Us}]), Exp - Now div 1000}
              || #{user := Us, verified := #{exp := Exp}} <- Sessions, is_integer(Exp)])},
     {"rabbitmq_access_users", gauge,
      "Accounts by activity state, cluster-wide (identical on every node)",
      [{[{"state", St}], N} || {St, N} <- user_states()]},
     {"rabbitmq_access_events_dropped_total", counter,
      "Events dropped by this node because the collector fell behind",
      [{[], counter_value(dropped_total)}]},
     {"rabbitmq_access_journal_bytes", gauge,
      "Size of this node's access journal",
      [{[], journal_bytes()}]}].

render_text(Metrics) ->
    [[<<"# HELP ">>, Name, <<" ">>, Help, <<"\n# TYPE ">>, Name, <<" ">>, atom_to_list(Type), <<"\n">>,
      [sample_text(Name, Type, S) || S <- Samples]]
     || {Name, Type, Help, Samples} <- Metrics].

%%----------------------------------------------------------------------------
%% Keeps the collector registered with rabbitmq_prometheus while it runs
%% (it can be enabled or disabled at any time).

init([]) ->
    self() ! check,
    {ok, #{registered => false, checks => 0}}.

handle_call(_, _, S) -> {reply, ok, S}.
handle_cast(_, S) -> {noreply, S}.

handle_info(check, S = #{registered := Reg, checks := N}) ->
    %% often at first, while plugins are still starting
    erlang:send_after(case N < 30 of true -> 2000; false -> ?CHECK_MS end, self(), check),
    Want = lists:member(export_mode(), [prometheus, both]) andalso prometheus_running(),
    {noreply, S#{registered => set_registered(Want, Reg), checks => N + 1}};
handle_info(_, S) ->
    {noreply, S}.

terminate(_, #{registered := true}) ->
    catch prometheus_registry:deregister_collector(default, ?COLLECTOR),
    ok;
terminate(_, _) ->
    ok.

set_registered(true, false) ->
    try ok = prometheus_registry:register_collector(default, ?COLLECTOR), true
    catch _:_ -> false
    end;
set_registered(false, true) ->
    catch prometheus_registry:deregister_collector(default, ?COLLECTOR),
    false;
set_registered(true, true) ->
    %% re-register if rabbitmq_prometheus was restarted and lost it
    try prometheus_registry:collector_registeredp(default, ?COLLECTOR) of
        true -> true;
        false -> set_registered(true, false)
    catch _:_ -> false
    end;
set_registered(false, false) -> false.

prometheus_running() ->
    lists:keymember(rabbitmq_prometheus, 1, application:which_applications()).

%%----------------------------------------------------------------------------

sum_by(Pairs) ->
    maps:to_list(lists:foldl(fun({L, N}, A) -> A#{L => maps:get(L, A, 0) + N} end, #{}, Pairs)).

min_by(Pairs) ->
    maps:to_list(lists:foldl(fun({L, N}, A) -> A#{L => min(N, maps:get(L, A, N))} end, #{}, Pairs)).

histograms(Pairs) ->
    Merged = lists:foldl(fun({L, {C, Sum, N}}, A) ->
                             case maps:find(L, A) of
                                 error -> A#{L => {C, Sum, N}};
                                 {ok, {C0, S0, N0}} ->
                                     A#{L => {list_to_tuple([element(I, C0) + element(I, C)
                                                             || I <- lists:seq(1, tuple_size(C))]),
                                              S0 + Sum, N0 + N}}
                             end
                         end, #{}, Pairs),
    [{L, lists:zip(?BUCKETS, tuple_to_list(C)) ++ [{infinity, N}], N, SumMs / 1000}
     || {L, {C, SumMs, N}} <- maps:to_list(Merged)].

user_states() ->
    try
        #{users := #{by_state := S}} = cached_overview(),
        lists:sort(maps:to_list(S))
    catch _:_ -> []
    end.

%% The cluster view is not recomputed on every scrape.
cached_overview() ->
    Now = rabbit_access_insight_util:now_ms(),
    case ets:lookup(?T_STATE, overview_cache) of
        [{_, T, O}] when Now - T < 30000 -> O;
        _ ->
            O = rabbit_access_insight_query:overview(),
            ets:insert(?T_STATE, {overview_cache, Now, O}),
            O
    end.

counter_value(K) ->
    case ets:lookup(?T_STATE, K) of [{_, N}] -> N; _ -> 0 end.

journal_bytes() ->
    try maps:get(bytes, rabbit_access_insight_journal:info()) catch _:_ -> 0 end.

sample_text(Name, Type, {Labels, Value}) when Type =:= gauge; Type =:= counter ->
    [Name, labels_text(Labels), <<" ">>, num(Value), <<"\n">>];
sample_text(Name, histogram, {Labels, Buckets, Count, Sum}) ->
    [[[Name, <<"_bucket">>, labels_text(Labels ++ [{"le", le(B)}]), <<" ">>, num(C), <<"\n">>]
      || {B, C} <- Buckets],
     Name, <<"_sum">>, labels_text(Labels), <<" ">>, num(Sum), <<"\n">>,
     Name, <<"_count">>, labels_text(Labels), <<" ">>, num(Count), <<"\n">>].

labels_text([]) -> <<>>;
labels_text(Labels) ->
    [<<"{">>, lists:join(<<",">>, [[K, <<"=\"">>, escape(V), <<"\"">>] || {K, V} <- Labels]), <<"}">>].

le(infinity) -> <<"+Inf">>;
le(B) -> num(B).

escape(V) ->
    B = rabbit_access_insight_util:bin(V),
    binary:replace(binary:replace(binary:replace(B, <<"\\">>, <<"\\\\">>, [global]),
                                  <<"\"">>, <<"\\\"">>, [global]), <<"\n">>, <<"\\n">>, [global]).

num(I) when is_integer(I) -> integer_to_binary(I);
num(F) when is_float(F) -> float_to_binary(F, [short]).
