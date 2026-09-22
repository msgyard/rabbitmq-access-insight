%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 martinx
%% SPDX-License-Identifier: MPL-2.0
%%
%% Replication of account aggregates between cluster nodes.
%%
%% Every node's contribution -- its origin {Node, Epoch} -- has exactly one
%% writer: that node's collector. Everything here copies contributions of
%% other origins, and never writes rows of the local node's current origin.
%%
%%   * Each round, for every running peer: fetch its digest
%%     #{Origin => Version} and tombstones; apply tombstones; for every origin
%%     the peer holds a newer version of, pull the rows changed since the
%%     version held here.
%%   * A row is stored only if it is new or carries a higher `v` than the
%%     copy held here, so duplicates and reordering never move a copy back.
%%   * forget_node/1 deletes every origin of a node that has left the
%%     cluster, everywhere, and leaves a tombstone so no peer re-sends it.
%%   * After an unclean stop the collector asks for its own origin back
%%     (reclaim/1): peers may hold changes it applied but had not yet made
%%     durable. Those rows are handed to the collector, which applies them
%%     itself so they cannot race its own writes.
-module(rabbit_access_insight_sync).
-behaviour(gen_server).

-include("rabbit_access_insight.hrl").
-include_lib("rabbit_common/include/logging.hrl").
-include_lib("kernel/include/logger.hrl").

-export([start_link/0, sync_now/1, reclaim/1, forget_node/1, status/0]).
%% called by peers
-export([digest/0, entries_since/2, forget_origins/1]).
%% used by the collector and queries
-export([store_rows/3, peers/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(RPC_TIMEOUT, 10000).
-define(LOG_META, #{domain => ?RMQLOG_DOMAIN_GLOBAL}).
-define(MIN_ON_DEMAND_MS, 5000).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% Pull from every running peer now, unless that was done less than five
%% seconds ago. Used before answering a query.
-spec sync_now(timeout()) -> ok.
sync_now(Timeout) ->
    try rabbit_access_insight_config:get(replication_enabled) =:= true
            andalso gen_server:call(?MODULE, sync_now, Timeout),
        ok
    catch exit:{timeout, _} -> ok;
          exit:{noproc, _} -> ok
    end.

reclaim(Origin) -> gen_server:cast(?MODULE, {reclaim, Origin}).

-spec forget_node(node()) -> {ok, [term()]} | {error, term()}.
forget_node(Node) -> gen_server:call(?MODULE, {forget_node, Node}, 60000).

status() -> gen_server:call(?MODULE, status).

%%----------------------------------------------------------------------------
%% Called by peers

digest() ->
    #{origins => maps:from_list([{O, maps:get(v, M, 0)} || {O, M} <- ets:tab2list(?T_ORIGIN)]),
      tombs => maps:from_list(ets:tab2list(?T_TOMB))}.

%% Rows of Origin changed after version Since. The origin row is read first:
%% a row read afterwards can only be newer, never older, than that version.
entries_since(Origin, Since) ->
    case ets:lookup(?T_ORIGIN, Origin) of
        [] -> not_found;
        [{_, Meta}] ->
            #{meta => Meta,
              user  => changed(?T_USER,  {Origin, '_'}, Since),
              daily => changed(?T_DAILY, {Origin, '_', '_', '_'}, Since),
              fail  => changed(?T_FAIL,  {Origin, '_', '_', '_', '_'}, Since)}
    end.

%% (map_get in match specs needs OTP 26; filter here to run on OTP 25.)
changed(Tab, KeyPattern, Since) ->
    [Row || Row = {_, #{v := V}} <- ets:select(Tab, [{{KeyPattern, '_'}, [], ['$_']}]), V > Since].

forget_origins(Origins) ->
    Now = rabbit_access_insight_util:now_ms(),
    [tomb(O, Now) || O <- Origins],
    ok.

%%----------------------------------------------------------------------------

init([]) ->
    schedule(),
    {ok, #{last_sync => 0, rounds => 0, errors => #{}, last_round_ms => 0}}.

handle_call(sync_now, _From, S = #{last_sync := Last}) ->
    Now = rabbit_access_insight_util:now_ms(),
    case Now - Last < ?MIN_ON_DEMAND_MS of
        true  -> {reply, ok, S};
        false -> {reply, ok, do_round(S)}
    end;
handle_call({forget_node, Node}, _From, S) ->
    {reply, do_forget(Node), S};
handle_call(status, _From, S) ->
    {reply, maps:with([last_sync, rounds, errors, last_round_ms], S), S}.

handle_cast({reclaim, Origin}, S) ->
    do_reclaim(Origin),
    {noreply, S}.

handle_info(round, S) ->
    schedule(),
    case rabbit_access_insight_config:get(replication_enabled) of
        true -> {noreply, do_round(S)};
        _    -> {noreply, S}
    end;
handle_info(_, S) ->
    {noreply, S}.

terminate(_, _) -> ok.

schedule() ->
    Interval = rabbit_access_insight_config:get(replication_interval),
    erlang:send_after(Interval + rand:uniform(max(1, Interval div 10)), self(), round).

%%----------------------------------------------------------------------------

peers() ->
    try rabbit_nodes:list_running() -- [node()]
    catch _:_ -> nodes()
    end.

do_round(S = #{rounds := R}) ->
    T0 = erlang:monotonic_time(millisecond),
    Own = own_origin(),
    Errors = lists:foldl(fun(Peer, Acc) ->
                             try pull(Peer, Own), Acc
                             catch C:E -> Acc#{Peer => iolist_to_binary(io_lib:format("~tp:~tp", [C, E]))}
                             end
                         end, #{}, peers()),
    prune_tombs(),
    S#{last_sync => rabbit_access_insight_util:now_ms(), rounds => R + 1, errors => Errors,
       last_round_ms => erlang:monotonic_time(millisecond) - T0}.

pull(Peer, Own) ->
    #{origins := Remote, tombs := Tombs} = erpc:call(Peer, ?MODULE, digest, [], ?RPC_TIMEOUT),
    Now = rabbit_access_insight_util:now_ms(),
    maps:foreach(fun(O, _) -> ets:member(?T_TOMB, O) orelse tomb(O, Now) end, Tombs),
    maps:foreach(
      fun(O, _) when O =:= Own -> ok;
         (O, RV) ->
              LV = rabbit_access_insight_model:origin_version(O),
              New = not ets:member(?T_ORIGIN, O),
              case (RV > LV orelse New) andalso not ets:member(?T_TOMB, O) of
                  false -> ok;
                  true ->
                      case erpc:call(Peer, ?MODULE, entries_since, [O, LV], ?RPC_TIMEOUT) of
                          not_found -> ok;
                          Entries -> store_rows(O, Entries, replica)
                      end
              end
      end, Remote).

%% Store rows received for Origin, each only if newer than the copy held.
store_rows(Origin, #{meta := Meta, user := U, daily := D, fail := F}, _Mode) ->
    [newer(?T_USER, Row) || Row <- U],
    [newer(?T_DAILY, Row) || Row <- D],
    [newer(?T_FAIL, Row) || Row <- F],
    case ets:lookup(?T_ORIGIN, Origin) of
        [{_, #{v := LV}}] when LV > map_get(v, Meta) -> ok;
        [{_, #{v := LV} = Local}] when LV =:= map_get(v, Meta) ->
            %% same version: keep the larger gap list / user count seen
            ets:insert(?T_ORIGIN, {Origin, maps:merge(Meta, maps:with([users], Local))});
        _ -> ets:insert(?T_ORIGIN, {Origin, Meta})
    end,
    ok.

newer(Tab, Row = {Key, #{v := V}}) ->
    case ets:lookup(Tab, Key) of
        [{_, #{v := LV}}] when LV >= V -> ok;
        _ -> ets:insert(Tab, Row)
    end.

do_reclaim(Origin) ->
    LV = rabbit_access_insight_model:origin_version(Origin),
    Best = lists:foldl(fun(Peer, Acc = {BV, _}) ->
                           try erpc:call(Peer, ?MODULE, digest, [], ?RPC_TIMEOUT) of
                               #{origins := #{Origin := RV}} when RV > BV -> {RV, Peer};
                               _ -> Acc
                           catch _:_ -> Acc
                           end
                       end, {LV, none}, peers()),
    case Best of
        {_, none} -> ok;
        {RV, Peer} ->
            case erpc:call(Peer, ?MODULE, entries_since, [Origin, LV], ?RPC_TIMEOUT) of
                not_found -> ok;
                Entries ->
                    ?LOG_INFO("rabbitmq_access_insight: recovered changes up to version ~b "
                              "of this node's history from ~ts", [RV, Peer], ?LOG_META),
                    rabbit_access_insight_collector:reclaimed(Origin, Entries)
            end
    end.

do_forget(Node) ->
    case lists:member(Node, [node() | peers()]) of
        true -> {error, <<"node is running; only nodes that left the cluster can be forgotten">>};
        false ->
            Origins = [O || {O = {N, _}, _} <- ets:tab2list(?T_ORIGIN), N =:= Node],
            forget_origins(Origins),
            _ = erpc:multicall(peers(), ?MODULE, forget_origins, [Origins], ?RPC_TIMEOUT),
            ?LOG_INFO("rabbitmq_access_insight: forgot ~b history epoch(s) of ~ts",
                      [length(Origins), Node], ?LOG_META),
            {ok, Origins}
    end.

tomb(Origin, Now) ->
    ets:insert(?T_TOMB, {Origin, Now}),
    ets:delete(?T_ORIGIN, Origin),
    ets:match_delete(?T_USER, {{Origin, '_'}, '_'}),
    ets:match_delete(?T_DAILY, {{Origin, '_', '_', '_'}, '_'}),
    ets:match_delete(?T_FAIL, {{Origin, '_', '_', '_', '_'}, '_'}),
    ok.

%% Tombstones outlive every row they could have to stop.
prune_tombs() ->
    Days = rabbit_access_insight_config:get(history_rollup_days) + 30,
    Cutoff = rabbit_access_insight_util:now_ms() - Days * 86400000,
    ets:select_delete(?T_TOMB, [{{'_', '$1'}, [{'<', '$1', Cutoff}], [true]}]).

own_origin() ->
    try rabbit_access_insight_collector:origin() catch _:_ -> undefined end.
