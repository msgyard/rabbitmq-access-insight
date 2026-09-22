%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 martinx
%% SPDX-License-Identifier: MPL-2.0
%%
%% Turns this node's rabbit_event stream into journal records and aggregates.
%%
%% It owns the event handler on rabbit_event (added with add_sup_handler, so
%% a crashing handler is dropped by the manager and attached again here), the
%% aggregate tables, and the journal process. It is the only writer of the
%% local origin's rows.
%%
%% A login is tracked through RabbitMQ's own events:
%%
%%   user_authentication_failure             -> login_failed, stage credentials
%%   user_authentication_success, then
%%     connection_created (same name)        -> session_open
%%     connection_closed without created     -> login_failed, stage access
%%   connection_closed of an open session    -> session_close
%%   access_auth_verified (same pid)         -> the method, confirmed
%%
%% On start it restores the last snapshot, replays the journal after it,
%% closes sessions that did not survive a restart, and records the sessions
%% already open (for example when the plugin is enabled on a running node).
-module(rabbit_access_insight_collector).
-behaviour(gen_server).

-include("rabbit_access_insight.hrl").
-include_lib("rabbit_common/include/logging.hrl").
-include_lib("kernel/include/logger.hrl").

-export([start_link/0, origin/0, seq/0, sync/0, snapshot_now/0, counts/0, local_status/0,
         reclaimed/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-import(rabbit_access_insight_util, [prop/2, prop/3, bin/1, bin/2, ip/1, now_ms/0]).

-define(HANDLER, rabbit_access_insight_handler).
-define(LOG_META, #{domain => ?RMQLOG_DOMAIN_GLOBAL}).
-define(FLUSH_MS, 200).
-define(FLUSH_MAX, 500).
-define(TICK_MS, 1000).
-define(UNCLEAN_SEQ_GAP, 10000000).
-define(DURATION_BUCKETS, [1, 10, 60, 300, 1800, 3600, 21600, 86400]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

origin() -> gen_server:call(?MODULE, origin).
seq() -> gen_server:call(?MODULE, seq).
%% Hand buffered records to the journal and make them durable.
sync() -> gen_server:call(?MODULE, sync, 30000).
snapshot_now() -> gen_server:call(?MODULE, snapshot_now, 60000).
local_status() -> gen_server:call(?MODULE, status, 10000).

%% Rows of this node's own origin recovered from a peer after an unclean stop.
reclaimed(Origin, Entries) -> gen_server:cast(?MODULE, {reclaimed, Origin, Entries}).

%% Events seen on this node since the collector started, by event type.
counts() ->
    lists:sort([{T, N} || {{seen, T}, N} <- ets:tab2list(?T_STATE)]).

%%----------------------------------------------------------------------------

init([]) ->
    process_flag(trap_exit, true),
    Dir = rabbit_access_insight_config:history_dir(),
    ok = rabbit_access_insight_model:create_tables(),
    %% Attach first: events that arrive while we recover wait in the mailbox.
    ok = attach(),
    {Epoch, NewEpoch} = rabbit_access_insight_store:epoch(Dir),
    Origin = {node(), Epoch},
    Clean = rabbit_access_insight_store:take_clean(Dir),
    {ok, _Journal} = rabbit_access_insight_journal:start_link(Dir),
    Limits = limits(),
    Now = now_ms(),
    S0 = #{dir => Dir, origin => Origin, seq => 0, last_ts => Now, vm => rabbit_access_insight_util:vm_id(),
           pending => #{}, verified => #{}, buffer => [], limits => Limits,
           disk_paused => false, skipped => 0, skipped_from => undefined,
           started_at => Now, recovered => #{}},
    S1 = restore(S0, NewEpoch),
    ok = rabbit_access_insight_model:new_origin(Origin, Now),
    S2 = case {NewEpoch, Clean} of
             {true, _} -> S1;
             {false, {clean, _}} -> S1;
             {false, unclean} ->
                 %% Replicas may hold changes this node applied but had not yet
                 %% made durable; never reuse their sequence numbers.
                 #{seq := Seq, last_ts := Last} = S1,
                 emit(gap, #{kind => unclean_shutdown, from => Last, to => Now},
                      S1#{seq => Seq + ?UNCLEAN_SEQ_GAP})
         end,
    StoppedAt = case Clean of {clean, T} -> T; unclean -> maps:get(last_ts, S2) end,
    S3 = bootstrap(close_stale(StoppedAt, S2)),
    _ = rabbit_access_insight_model:prune_daily(Now, rabbit_access_insight_config:get(history_rollup_days)),
    S4 = flush(S3),
    Clean =:= unclean andalso not NewEpoch andalso
        erlang:send_after(15000, self(), reclaim),
    erlang:send_after(?FLUSH_MS, self(), flush),
    erlang:send_after(?TICK_MS, self(), tick),
    erlang:send_after(rabbit_access_insight_config:get(snapshot_interval), self(), snapshot),
    ?LOG_INFO("rabbitmq_access_insight: history ~ts, epoch ~ts, sequence ~b~ts",
              [Dir, Epoch, maps:get(seq, S4),
               case {NewEpoch, Clean} of
                   {true, _} -> " (new history)";
                   {_, unclean} -> " (recovered after an unclean stop)";
                   _ -> ""
               end], ?LOG_META),
    {ok, S4#{recovered => #{new_epoch => NewEpoch, clean => Clean =/= unclean}}}.

handle_call(origin, _From, S = #{origin := O}) ->
    {reply, O, S};
handle_call(seq, _From, S = #{seq := Seq}) ->
    {reply, Seq, S};
handle_call(sync, _From, S) ->
    S1 = flush(S),
    {reply, rabbit_access_insight_journal:sync(), S1};
handle_call(snapshot_now, _From, S) ->
    S1 = flush(S),
    ok = rabbit_access_insight_journal:sync(),
    ok = write_snapshot(S1),
    {reply, ok, S1};
handle_call(status, _From, S) ->
    #{origin := {_, Epoch}, seq := Seq, pending := P, verified := V, disk_paused := DP,
      started_at := St, recovered := R, dir := Dir} = S,
    {reply, #{epoch => Epoch, seq => Seq, pending_logins => map_size(P),
              verified_waiting => map_size(V), disk_paused => DP, started_at => St,
              recovered => R, history_dir => bin(Dir, 1024),
              open_sessions => ets:info(?T_SESSION, size),
              journal => rabbit_access_insight_journal:info()}, S};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({reclaimed, Origin, Entries}, S = #{origin := Origin}) ->
    %% Applied here, in the only writer of this origin, and only where newer.
    ok = rabbit_access_insight_sync:store_rows(Origin, Entries, reclaim),
    {noreply, S};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({event, Type, Props, Ts}, S) ->
    _ = ets:update_counter(?T_STATE, {seen, Type}, 1, {{seen, Type}, 0}),
    S1 = try on_event(Type, Props, Ts, S)
         catch C:E:St ->
             ?LOG_WARNING("rabbitmq_access_insight: could not process ~tp event: ~tp:~tp ~tp",
                          [Type, C, E, St], ?LOG_META),
             S
         end,
    {noreply, maybe_flush(S1)};
handle_info(flush, S) ->
    erlang:send_after(?FLUSH_MS, self(), flush),
    {noreply, flush(S)};
handle_info(tick, S) ->
    erlang:send_after(?TICK_MS, self(), tick),
    {noreply, tick(S)};
handle_info(snapshot, S) ->
    erlang:send_after(rabbit_access_insight_config:get(snapshot_interval), self(), snapshot),
    S1 = flush(S),
    Snap = snapshot_term(S1),
    Dir = maps:get(dir, S1),
    _ = spawn(fun() -> rabbit_access_insight_store:save_snapshot(Dir, Snap) end),
    {noreply, S1};
handle_info({gen_event_EXIT, ?HANDLER, normal}, S) ->
    {noreply, S};
handle_info({gen_event_EXIT, ?HANDLER, Reason}, S) ->
    ?LOG_WARNING("rabbitmq_access_insight: event handler exited (~tp); re-attaching",
                 [Reason], ?LOG_META),
    erlang:send_after(1000, self(), attach),
    {noreply, S};
handle_info(reclaim, S = #{origin := Origin}) ->
    catch rabbit_access_insight_sync:reclaim(Origin),
    {noreply, S};
handle_info(attach, S) ->
    ok = attach(),
    {noreply, S};
handle_info({'EXIT', _Pid, normal}, S) ->
    {noreply, S};
handle_info({'EXIT', _Pid, Reason}, S) ->
    {stop, Reason, S};
handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, S) ->
    try
        S1 = flush(S),
        ok = rabbit_access_insight_journal:sync(),
        ok = write_snapshot(S1),
        ok = rabbit_access_insight_journal:close(),
        ok = rabbit_access_insight_store:mark_clean(maps:get(dir, S1), now_ms())
    catch C:E ->
        ?LOG_WARNING("rabbitmq_access_insight: could not stop cleanly: ~tp:~tp", [C, E], ?LOG_META)
    end,
    ok.

%%----------------------------------------------------------------------------
%% Events

on_event(user_authentication_success, Props, Ts, S = #{pending := P}) ->
    case prop(connection_name, Props) of
        Name when is_binary(Name) ->
            S#{pending => P#{Name => {Ts, login_props(Props)}}};
        _ ->
            S   %% direct (in-broker) logins carry no connection name
    end;

on_event(user_authentication_failure, Props, _Ts, S) ->
    L = login_props(Props),
    ctr({auth, maps:get(user, L), <<"unknown">>, failed, credentials}),
    emit(login_failed, L#{stage => credentials,
                          reason => reason(prop(error, Props, <<"invalid credentials">>), S)}, S);

on_event(access_auth_verified, Props, Ts, S = #{verified := V}) ->
    case prop(pid, Props) of
        Pid when is_pid(Pid) -> S#{verified => V#{Pid => {Ts, verified_props(Props)}}};
        _ -> S
    end;

on_event(connection_created, Props, Ts, S) ->
    Pid = prop(pid, Props),
    case is_pid(Pid) andalso not ets:member(?T_SESSION, Pid) of
        false -> S;
        true  -> open_session(Pid, Props, Ts, false, S)
    end;

on_event(connection_closed, Props, Ts, S = #{pending := P, verified := V}) ->
    Pid = prop(pid, Props),
    Name = prop(name, Props),
    S1 = S#{verified => maps:remove(Pid, V)},
    case ets:lookup(?T_SESSION, Pid) of
        [{_, Sess}] ->
            ets:delete(?T_SESSION, Pid),
            close_session(Sess, Ts, closed, S1);
        [] ->
            case maps:take(Name, P) of
                {{_, L}, P1} ->
                    ctr({auth, maps:get(user, L), <<"unknown">>, failed, access}),
                    emit(login_failed,
                         L#{stage => access,
                            reason => <<"refused after authentication (authorization or virtual host access)">>},
                         S1#{pending => P1});
                error ->
                    S1
            end
    end;

on_event(_Type, _Props, _Ts, S) ->
    S.

login_props(Props) ->
    Name = prop(connection_name, Props),
    Peer = case prop(peer_host, Props) of
               undefined when is_binary(Name) -> rabbit_access_insight_util:conn_peer(Name);
               undefined -> <<"unknown">>;
               H -> ip(H)
           end,
    #{user      => bin(prop(name, Props, <<>>), 256),
      peer      => Peer,
      protocol  => protocol(Props),
      mechanism => bin(prop(auth_mechanism, Props, <<>>), 64),
      conn_name => bin(Name, 256)}.

protocol(Props) ->
    case {prop(protocol, Props), prop(connection_type, Props)} of
        {undefined, direct} -> <<"direct">>;
        {undefined, _} -> <<"unknown">>;
        {P, _} -> rabbit_access_insight_util:protocol(P)
    end.

verified_props(Props) ->
    Keep = [{credential, 64}, {alg, 32}, {iss, 128}, {kid, 128}, {backend, 128}],
    Base = #{method => bin(prop(method, Props, unknown), 32)},
    M1 = lists:foldl(fun({K, Max}, Acc) ->
                         case prop(K, Props) of
                             undefined -> Acc;
                             V -> Acc#{K => bin(V, Max)}
                         end
                     end, Base, Keep),
    lists:foldl(fun(K, Acc) ->
                    case prop(K, Props) of
                        V when is_integer(V) -> Acc#{K => V};
                        _ -> Acc
                    end
                end, M1, [exp, iat, nbf]).

reason(Error, #{limits := #{max_reason_bytes := Max}}) ->
    bin(Error, Max).

open_session(Pid, Props, Ts, Bootstrap, S = #{pending := P, verified := V, vm := Vm}) ->
    Name = prop(name, Props),
    {Login, P1} = case maps:take(Name, P) of
                      {{_, L}, Rest} -> {L, Rest};
                      error -> {#{}, P}
                  end,
    {Verified, V1} = case maps:take(Pid, V) of
                         {{_, Vf}, RestV} -> {Vf, RestV};
                         error -> {undefined, V}
                     end,
    User = bin(prop(user, Props, maps:get(user, Login, <<>>)), 256),
    Mechanism = bin(prop(auth_mechanism, Props, maps:get(mechanism, Login, <<>>)), 64),
    {Method, Source} = rabbit_access_insight_auth_method:resolve(Verified, Mechanism, User),
    Opened = case prop(connected_at, Props) of
                 C when is_integer(C) -> C;
                 _ -> Ts
             end,
    Sess0 = #{pid => rabbit_access_insight_util:pid_bin(Pid),
              user => User,
              vhost => bin(prop(vhost, Props, <<>>), 256),
              protocol => protocol(Props),
              peer => case prop(peer_host, Props) of
                          undefined -> rabbit_access_insight_util:conn_peer(Name);
                          H -> ip(H)
                      end,
              peer_port => case prop(peer_port, Props) of I when is_integer(I) -> I; _ -> 0 end,
              conn_name => bin(Name, 256),
              client => rabbit_access_insight_util:client_name(Props),
              type => bin(prop(type, Props, network), 32),
              mechanism => Mechanism,
              method => Method,
              method_source => Source,
              opened_at => Opened,
              bootstrap => Bootstrap},
    Sess = case Verified of
               undefined -> Sess0;
               _ -> Sess0#{verified => Verified}
           end,
    ets:insert(?T_SESSION, {Pid, Sess#{vm => Vm}}),
    ctr({opened, maps:get(vhost, Sess), User, Method, maps:get(protocol, Sess)}),
    ctr({auth, User, Method, success, none}),
    emit(session_open, Sess#{vm => Vm}, S#{pending => P1, verified => V1}).

close_session(Sess, Ts, Why, S) ->
    #{opened_at := Opened, vhost := VHost, user := User} = Sess,
    End = max(Ts, Opened),
    duration(VHost, User, End - Opened),
    Rec = maps:with([pid, user, vhost, protocol, peer, client, method, method_source,
                     opened_at, conn_name], Sess),
    emit(session_close, Rec#{duration_ms => End - Opened, reason => Why,
                             estimated => Why =/= closed}, S#{last_ts => max(End, maps:get(last_ts, S))}).

%% Sessions recorded before a restart whose connection is gone are closed at
%% the last moment the node is known to have been up.
close_stale(StoppedAt, S = #{vm := Vm}) ->
    Live = live_pids(),
    lists:foldl(fun({Pid, Sess}, Acc) ->
                    Alive = maps:get(vm, Sess, undefined) =:= Vm andalso sets:is_element(Pid, Live),
                    case Alive of
                        true -> Acc;
                        false ->
                            ets:delete(?T_SESSION, Pid),
                            End = case maps:get(vm, Sess, undefined) =:= Vm of
                                      true  -> now_ms();     %% same VM: the collector restarted
                                      false -> StoppedAt     %% the node restarted
                                  end,
                            close_session(Sess, max(End, maps:get(opened_at, Sess)),
                                          ended_while_not_observed, Acc)
                    end
                end, S, ets:tab2list(?T_SESSION)).

%% Connections that are open but not yet known: the plugin was just enabled,
%% or the collector restarted.
bootstrap(S) ->
    lists:foldl(fun(TC, Acc) ->
                    {Pid, Props} = tracked_props(TC),
                    case is_pid(Pid) andalso not ets:member(?T_SESSION, Pid) of
                        true  -> open_session(Pid, Props, now_ms(), true, Acc);
                        false -> Acc
                    end
                end, S, tracked_connections()).

tracked_connections() ->
    try rabbit_connection_tracking:list_on_node(node())
    catch _:_ -> []
    end.

live_pids() ->
    sets:from_list([element(1, tracked_props(TC)) || TC <- tracked_connections()]).

%% #tracked_connection{id, node, vhost, name, pid, protocol, type, peer_host,
%% peer_port, username, connected_at} -- unchanged from 3.12 to 4.3.
tracked_props({tracked_connection, _Id, _Node, VHost, Name, Pid, Protocol, Type,
               PeerHost, PeerPort, User, ConnectedAt}) ->
    {Pid, [{pid, Pid}, {vhost, VHost}, {name, Name}, {protocol, Protocol}, {type, Type},
           {peer_host, PeerHost}, {peer_port, PeerPort}, {user, User},
           {connected_at, ConnectedAt}]};
tracked_props(_) ->
    {undefined, []}.

%%----------------------------------------------------------------------------
%% Records

emit(Type, Map, S = #{seq := Seq0, origin := Origin, buffer := Buf, limits := Limits}) ->
    Seq = Seq0 + 1,
    Ts = now_ms(),
    Rec = ?REC(Seq, Ts, Type, Map),
    ok = rabbit_access_insight_model:apply(Origin, Rec, Limits),
    ok = rabbit_access_insight_model:remember(Rec),
    S1 = S#{seq => Seq, last_ts => max(Ts, maps:get(last_ts, S))},
    case S1 of
        #{disk_paused := true, skipped := N} -> S1#{skipped => N + 1};
        _ -> S1#{buffer => [Rec | Buf]}
    end.

maybe_flush(S = #{buffer := Buf}) when length(Buf) >= ?FLUSH_MAX -> flush(S);
maybe_flush(S) -> S.

flush(S = #{buffer := []}) -> S;
flush(S = #{buffer := Buf}) ->
    ok = rabbit_access_insight_journal:append(lists:reverse(Buf)),
    S#{buffer => []}.

%%----------------------------------------------------------------------------
%% Once a second: expire unmatched logins, account for events the handler had
%% to drop, and follow the disk alarm.

tick(S0) ->
    Now = now_ms(),
    TTL = rabbit_access_insight_config:get(pending_ttl),
    S1 = expire(Now - TTL, S0),
    S2 = case ets:take(?T_STATE, dropped) of
             [{dropped, N, From}] when N > 0 ->
                 _ = ets:update_counter(?T_STATE, dropped_total, N, {dropped_total, 0}),
                 ?LOG_WARNING("rabbitmq_access_insight: dropped ~b events under load", [N], ?LOG_META),
                 emit(gap, #{kind => overload, count => N, from => From, to => Now}, S1);
             _ -> S1
         end,
    disk_alarm(disk_alarm_active(), Now, S2).

expire(Cutoff, S = #{pending := P, verified := V}) ->
    Keep = fun(_, {T, _}) -> T >= Cutoff end,
    P1 = maps:filter(Keep, P),
    Unpaired = map_size(P) - map_size(P1),
    Unpaired > 0 andalso ets:update_counter(?T_STATE, unpaired_logins, Unpaired, {unpaired_logins, 0}),
    S#{pending => P1, verified => maps:filter(Keep, V)}.

disk_alarm(true, Now, S = #{disk_paused := false}) ->
    ?LOG_WARNING("rabbitmq_access_insight: disk alarm, pausing the journal", [], ?LOG_META),
    flush(S#{disk_paused => true, skipped => 0, skipped_from => Now});
disk_alarm(false, Now, S = #{disk_paused := true, skipped := N, skipped_from := From}) ->
    ?LOG_INFO("rabbitmq_access_insight: disk alarm cleared, ~b records were not journaled", [N], ?LOG_META),
    emit(gap, #{kind => disk_alarm, count => N, from => From, to => Now},
         S#{disk_paused => false, skipped => 0, skipped_from => undefined});
disk_alarm(_, _Now, S) ->
    S.

disk_alarm_active() ->
    try rabbit_alarm:get_local_alarms() of
        Alarms -> lists:any(fun({{resource_limit, disk, _}, _}) -> true; (_) -> false end, Alarms)
    catch _:_ -> false
    end.

%%----------------------------------------------------------------------------
%% Snapshots and recovery

snapshot_term(#{origin := {_, Epoch}, seq := Seq, vm := Vm}) ->
    #{epoch => Epoch, seq => Seq, ts => now_ms(), vm => Vm,
      tables => maps:from_list([{T, ets:tab2list(T)}
                                || T <- ?AGG_TABLES ++ [?T_SESSION, ?T_CTR, ?T_RECENT]])}.

write_snapshot(S = #{dir := Dir}) ->
    rabbit_access_insight_store:save_snapshot(Dir, snapshot_term(S)).

restore(S, true) -> S;
restore(S = #{dir := Dir, origin := Origin = {_, Epoch}, limits := Limits}, false) ->
    {Seq0, Ts0} = case rabbit_access_insight_store:load_snapshot(Dir) of
                      {ok, #{epoch := Epoch, seq := Sq, ts := T, tables := Tabs}} ->
                          maps:foreach(fun(Tab, Rows) -> ets:insert(Tab, Rows) end, Tabs),
                          {Sq, T};
                      _ ->
                          {0, 0}
                  end,
    _ = rabbit_access_insight_model:new_origin(Origin, now_ms()),
    {Seq, Last, N} = replay(Seq0, Origin, Limits, {Seq0, Ts0, 0}),
    N > 0 andalso ?LOG_INFO("rabbitmq_access_insight: replayed ~b journal records after the snapshot",
                            [N], ?LOG_META),
    S#{seq => Seq, last_ts => max(Last, Ts0)}.

replay(Since, Origin, Limits, {MaxSeq, MaxTs, N}) ->
    case rabbit_access_insight_journal:read(Since, 10000) of
        [] -> {MaxSeq, MaxTs, N};
        Recs ->
            lists:foreach(fun(R) ->
                              rabbit_access_insight_model:apply(Origin, R, Limits),
                              rabbit_access_insight_model:remember(R),
                              replay_session(R)
                          end, Recs),
            ?REC(LastSeq, LastTs, _, _) = lists:last(Recs),
            replay(LastSeq, Origin, Limits, {max(MaxSeq, LastSeq), max(MaxTs, LastTs), N + length(Recs)})
    end.

%% The open-session table is rebuilt from the same records, so a session
%% opened or closed after the snapshot is known correctly after a crash.
replay_session(?REC(_, _, session_open, M = #{pid := P, vm := _})) ->
    case to_pid(P) of
        undefined -> ok;
        Pid -> ets:insert(?T_SESSION, {Pid, M})
    end;
replay_session(?REC(_, _, session_close, #{pid := P})) ->
    case to_pid(P) of
        undefined -> ok;
        Pid -> ets:delete(?T_SESSION, Pid)
    end;
replay_session(_) ->
    ok.

to_pid(Bin) ->
    try list_to_pid(binary_to_list(Bin)) catch _:_ -> undefined end.

%%----------------------------------------------------------------------------

limits() ->
    #{max_set => rabbit_access_insight_config:get(max_set),
      max_users => rabbit_access_insight_config:get(max_users),
      max_reason_bytes => rabbit_access_insight_config:get(max_reason_bytes)}.

attach() ->
    gen_event:add_sup_handler(rabbit_event, ?HANDLER, []).

ctr(Key) ->
    _ = ets:update_counter(?T_CTR, Key, 1, {Key, 0}),
    ok.

duration(VHost, User, Ms) ->
    Key = {duration, VHost, User},
    Secs = Ms / 1000,
    {Counts, Sum, N} = case ets:lookup(?T_CTR, Key) of
                           [{_, V}] -> V;
                           [] -> {list_to_tuple([0 || _ <- ?DURATION_BUCKETS]), 0, 0}
                       end,
    Counts1 = bucket(Secs, ?DURATION_BUCKETS, 1, Counts),
    ets:insert(?T_CTR, {Key, {Counts1, Sum + Ms, N + 1}}),
    ok.

%% Cumulative buckets: each bucket counts observations =< its bound.
bucket(_Secs, [], _I, Counts) -> Counts;
bucket(Secs, [B | Bs], I, Counts) when Secs =< B ->
    bucket(Secs, Bs, I + 1, setelement(I, Counts, element(I, Counts) + 1));
bucket(Secs, [_ | Bs], I, Counts) ->
    bucket(Secs, Bs, I + 1, Counts).
