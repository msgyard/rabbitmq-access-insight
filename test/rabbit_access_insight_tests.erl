%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 martinx
%% SPDX-License-Identifier: MPL-2.0
%%
-module(rabbit_access_insight_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include("../src/rabbit_access_insight.hrl").

-define(C, rabbit_access_insight_collector).
-define(LIMITS, #{max_set => 3, max_users => 100, max_reason_bytes => 40}).

%%----------------------------------------------------------------------------
%% util

days_between_splits_at_utc_midnight_test() ->
    D = 86400000,
    T0 = 20000 * D,                                  %% 2024-10-04 00:00 UTC
    ?assertEqual([], rabbit_access_insight_util:days_between(T0, T0)),
    ?assertEqual([{<<"2024-10-04">>, 1000}], rabbit_access_insight_util:days_between(T0, T0 + 1000)),
    ?assertEqual([{<<"2024-10-04">>, 1000}, {<<"2024-10-05">>, D}, {<<"2024-10-06">>, 5}],
                 rabbit_access_insight_util:days_between(T0 + D - 1000, T0 + 2 * D + 5)).

ip_formats_mapped_ipv4_test() ->
    ?assertEqual(<<"127.0.0.1">>, rabbit_access_insight_util:ip({0,0,0,0,0,65535,32512,1})),
    ?assertEqual(<<"10.1.2.3">>, rabbit_access_insight_util:ip({10,1,2,3})),
    ?assertEqual(<<"::1">>, rabbit_access_insight_util:ip({0,0,0,0,0,0,0,1})).

conn_peer_test() ->
    ?assertEqual(<<"10.0.0.7">>, rabbit_access_insight_util:conn_peer(<<"10.0.0.7:5555 -> 10.0.0.1:5672">>)),
    ?assertEqual(<<"unknown">>, rabbit_access_insight_util:conn_peer(undefined)).

protocol_names_test() ->
    P = fun rabbit_access_insight_util:protocol/1,
    ?assertEqual(<<"AMQP 0-9-1">>, P({0,9,1})),
    ?assertEqual(<<"MQTT 3.1.1">>, P({'MQTT', {3,1,1}})),
    ?assertEqual(<<"STOMP 1.2">>, P({'STOMP', "1.2"})),
    ?assertEqual(<<"AMQP 1.0">>, P({'AMQP', {1,0}})).

client_name_prefers_user_provided_name_test() ->
    CP = [{<<"product">>, longstr, <<"pika">>}, {<<"version">>, longstr, <<"1.3">>}],
    ?assertEqual(<<"app-a">>, rabbit_access_insight_util:client_name([{user_provided_name, <<"app-a">>}, {client_properties, CP}])),
    ?assertEqual(<<"pika 1.3">>, rabbit_access_insight_util:client_name([{client_properties, CP}])),
    ?assertEqual(<<"c1">>, rabbit_access_insight_util:client_name([{client_properties, [{client_id, longstr, <<"c1">>}]}])),
    ?assertEqual(<<"unknown">>, rabbit_access_insight_util:client_name([])).

bin_cuts_on_utf8_boundary_test() ->
    ?assertEqual(<<"ab">>, rabbit_access_insight_util:bin(<<"ab", 16#e4, 16#b8, 16#ad>>, 4)),
    ?assertEqual(<<"x">>, rabbit_access_insight_util:bin("x")).

%%----------------------------------------------------------------------------
%% auth method

only_internal_test() ->
    I = fun rabbit_access_insight_auth_method:only_internal/1,
    ?assert(I([rabbit_auth_backend_internal])),
    ?assert(I([{rabbit_auth_backend_internal, rabbit_auth_backend_internal}])),
    ?assertNot(I([rabbit_auth_backend_internal, {rabbit_auth_backend_http, rabbit_auth_backend_internal}])),
    ?assertNot(I([rabbit_auth_backend_ldap])),
    ?assertNot(I([])).

confirmed_method_wins_test() ->
    ?assertEqual({<<"token">>, <<"confirmed">>},
                 rabbit_access_insight_auth_method:resolve(#{method => <<"token">>}, <<"PLAIN">>, <<"u">>)),
    ?assertEqual({<<"certificate">>, <<"inferred">>},
                 rabbit_access_insight_auth_method:resolve(undefined, <<"EXTERNAL">>, <<"u">>)).

%%----------------------------------------------------------------------------
%% model

model_test_() ->
    {setup, fun() -> rabbit_access_insight_model:create_tables() end,
     fun(_) -> [ets:delete(T) || T <- ?AGG_TABLES ++ ?LOCAL_TABLES] end,
     fun(_) ->
         O = {n1, <<"e">>},
         rabbit_access_insight_model:new_origin(O, 0),
         D = 86400000, T0 = 20000 * D,
         A = fun(R) -> rabbit_access_insight_model:apply(O, R, ?LIMITS) end,
         A(?REC(1, T0 + 10, session_open, #{user => <<"u">>, vhost => <<"/">>, method => <<"password">>,
                                            peer => <<"1.1.1.1">>, client => <<"c">>, protocol => <<"p">>})),
         A(?REC(2, T0 + D + 10, session_close, #{user => <<"u">>, vhost => <<"/">>, opened_at => T0 + 10})),
         A(?REC(3, T0 + D + 20, login_failed, #{user => <<"u">>, stage => credentials, peer => <<"2.2.2.2">>,
                                               reason => <<"bad">>})),
         A(?REC(4, T0 + D + 30, login_failed, #{user => <<"u">>, stage => access, peer => <<"2.2.2.2">>,
                                               reason => <<"vhost">>, vhost => <<"/">>})),
         [{_, U}] = ets:lookup(?T_USER, {O, <<"u">>}),
         Daily = lists:sort(ets:tab2list(?T_DAILY)),
         [{_, Orig}] = ets:lookup(?T_ORIGIN, O),
         [?_assertEqual(1, maps:get(sessions, U)),
          ?_assertEqual(D, maps:get(online_ms, U)),
          ?_assertEqual(T0 + 10, maps:get(first_seen, U)),
          ?_assertEqual(T0 + D + 10, maps:get(last_seen, U)),
          ?_assertEqual(1, maps:get(failed, U)),
          ?_assertEqual(1, maps:get(refused, U)),
          ?_assertEqual(4, maps:get(v, U)),
          ?_assertEqual(4, maps:get(v, Orig)),
          %% online time is split across the two UTC days
          ?_assertEqual([D - 10, 10],
                        [maps:get(online_ms, M, 0) || {{_, <<"u">>, <<"/">>, _}, M} <- Daily]),
          ?_assertEqual(2, ets:info(?T_FAIL, size))]
     end}.

model_bounds_sets_and_users_test_() ->
    {setup, fun() -> rabbit_access_insight_model:create_tables() end,
     fun(_) -> [ets:delete(T) || T <- ?AGG_TABLES ++ ?LOCAL_TABLES] end,
     fun(_) ->
         O = {n1, <<"e">>},
         rabbit_access_insight_model:new_origin(O, 0),
         L = ?LIMITS#{max_users => 2},
         [rabbit_access_insight_model:apply(O, ?REC(I, 1000, session_open,
             #{user => <<"u">>, vhost => <<"/">>, peer => integer_to_binary(I)}), L) || I <- lists:seq(1, 5)],
         [rabbit_access_insight_model:apply(O, ?REC(10 + I, 1000, login_failed,
             #{user => integer_to_binary(I), stage => credentials}), L) || I <- lists:seq(1, 5)],
         [{_, U}] = ets:lookup(?T_USER, {O, <<"u">>}),
         Users = lists:sort([Name || {{_, Name}, _} <- ets:tab2list(?T_USER)]),
         [?_assertEqual(#{<<"1">> => 1, <<"2">> => 1, <<"3">> => 1, ?OTHER => 2}, maps:get(sources, U)),
          ?_assertEqual([?OTHER, <<"1">>, <<"u">>], Users)]
     end}.

merge_user_test() ->
    A = #{sessions => 1, first_seen => 5, last_seen => 9, sources => #{<<"x">> => 1}, v => 3},
    B = #{sessions => 2, first_seen => 3, last_seen => 7, sources => #{<<"x">> => 1, <<"y">> => 4}, v => 8},
    ?assertEqual(#{sessions => 3, first_seen => 3, last_seen => 9,
                   sources => #{<<"x">> => 2, <<"y">> => 4}, v => 8},
                 rabbit_access_insight_model:merge_user(A, B)).

%%----------------------------------------------------------------------------
%% replication rows

sync_rows_test_() ->
    {setup, fun() -> rabbit_access_insight_model:create_tables() end,
     fun(_) -> [ets:delete(T) || T <- ?AGG_TABLES ++ ?LOCAL_TABLES] end,
     fun(_) ->
         O = {n2, <<"e2">>},
         rabbit_access_insight_model:new_origin(O, 0),
         L = ?LIMITS,
         [rabbit_access_insight_model:apply(O, ?REC(S, 1000 + S, session_open,
             #{user => U, vhost => <<"/">>}), L) || {S, U} <- [{1, <<"a">>}, {2, <<"b">>}, {3, <<"a">>}]],
         All = rabbit_access_insight_sync:entries_since(O, 0),
         Since2 = rabbit_access_insight_sync:entries_since(O, 2),
         %% store a stale copy of "a" (v=1) over the current one (v=3): ignored
         Stale = {{O, <<"a">>}, #{v => 1, sessions => 1}},
         ok = rabbit_access_insight_sync:store_rows(O, #{meta => #{v => 1}, user => [Stale], daily => [], fail => []}, replica),
         [{_, A}] = ets:lookup(?T_USER, {O, <<"a">>}),
         [{_, Meta}] = ets:lookup(?T_ORIGIN, O),
         [?_assertEqual(2, length(maps:get(user, All))),
          ?_assertEqual([<<"a">>], [U || {{_, U}, _} <- maps:get(user, Since2)]),
          ?_assertEqual(not_found, rabbit_access_insight_sync:entries_since({x, <<"y">>}, 0)),
          ?_assertEqual(2, maps:get(sessions, A)),
          ?_assertEqual(3, maps:get(v, Meta))]
     end}.

%%----------------------------------------------------------------------------
%% output formats

metrics_text_test() ->
    T = iolist_to_binary(rabbit_access_insight_metrics:render_text(
          [{"m_total", counter, "help", [{[{"user", <<"a\"b">>}], 3}]},
           {"h", histogram, "hh", [{[{"vhost", <<"/">>}], [{1, 1}, {infinity, 2}], 2, 1.5}]}])),
    ?assertNotEqual(nomatch, binary:match(T, <<"# TYPE m_total counter\n">>)),
    ?assertNotEqual(nomatch, binary:match(T, <<"m_total{user=\"a\\\"b\"} 3\n">>)),
    ?assertNotEqual(nomatch, binary:match(T, <<"h_bucket{vhost=\"/\",le=\"+Inf\"} 2\n">>)),
    ?assertNotEqual(nomatch, binary:match(T, <<"h_sum{vhost=\"/\"} 1.5\n">>)).

csv_guards_formulas_and_quotes_test() ->
    Row = #{name => <<"=cmd()">>, defined => true, tags => [<<"a,b">>], has_password => true,
            state => never_used, connected => 0, sessions => 0, first_seen => null, last_seen => null,
            failed => 0, refused => 0, methods => #{}, method_sources => #{}, sources => []},
    Csv = iolist_to_binary(rabbit_access_insight_report:csv([Row])),
    ?assertNotEqual(nomatch, binary:match(Csv, <<"'=cmd()">>)),
    ?assertNotEqual(nomatch, binary:match(Csv, <<"\"a,b\"">>)).

json_clean_test() ->
    J = rabbit_json:decode(iolist_to_binary(rabbit_access_insight_api:json(
          #{a => undefined, b => {1, 2}, c => "text", d => [<<"x">>], e => []}))),
    ?assertEqual(#{<<"a">> => null, <<"b">> => [1, 2], <<"c">> => <<"text">>,
                   <<"d">> => [<<"x">>], <<"e">> => []}, J).

%%----------------------------------------------------------------------------
%% journal

journal_test_() ->
    {setup,
     fun() ->
         Dir = tmp_dir(),
         %% 2 MiB budget -> 1 MiB segments; 3 MB of records must drop the oldest
         application:set_env(rabbitmq_access_insight, history_max_disk, 2 * 1024 * 1024),
         {ok, Pid} = rabbit_access_insight_journal:start_link(Dir),
         unlink(Pid),
         {Dir, Pid}
     end,
     fun({_Dir, Pid}) ->
         gen_server:stop(Pid),
         application:unset_env(rabbitmq_access_insight, history_max_disk)
     end,
     fun({_Dir, _Pid}) ->
         Pad = binary:copy(<<"x">>, 1000),
         Now = erlang:system_time(millisecond),
         [ok = rabbit_access_insight_journal:append([?REC(S, Now, t, #{pad => Pad}) || S <- lists:seq(B, B + 99)])
          || B <- lists:seq(1, 3000, 100)],
         All = rabbit_access_insight_journal:read(0, infinity),
         #{oldest_seq := Oldest, bytes := Bytes} = rabbit_access_insight_journal:info(),
         First = element(1, hd(All)),
         Mid = rabbit_access_insight_journal:read(First + 10, 10),
         [?_assert(First > 1),                         %% the oldest segment was dropped
          ?_assertEqual(Oldest, First),
          ?_assert(Bytes =< 2 * 1024 * 1024 + 1024 * 1024),
          %% whatever survived retention is contiguous and ends at 3000
          ?_assertEqual(lists:seq(First, 3000), [element(1, R) || R <- All]),
          ?_assertEqual(lists:seq(First + 11, First + 20), [element(1, R) || R <- Mid])]
     end}.

journal_drops_records_past_retention_test_() ->
    {setup,
     fun() ->
         application:set_env(rabbitmq_access_insight, history_max_disk, 2 * 1024 * 1024),
         {ok, Pid} = rabbit_access_insight_journal:start_link(tmp_dir()),
         unlink(Pid),
         Pid
     end,
     fun(Pid) ->
         gen_server:stop(Pid),
         application:unset_env(rabbitmq_access_insight, history_max_disk)
     end,
     fun(_) ->
         Pad = binary:copy(<<"z">>, 1000),
         Old = erlang:system_time(millisecond) - 31 * 86400000,
         Now = erlang:system_time(millisecond),
         %% ~1.2 MB of 31-day-old records, then enough new ones to rotate
         [ok = rabbit_access_insight_journal:append([?REC(S, Old, t, #{pad => Pad})]) || S <- lists:seq(1, 1200)],
         [ok = rabbit_access_insight_journal:append([?REC(S, Now, t, #{pad => Pad})]) || S <- lists:seq(1201, 1300)],
         All = rabbit_access_insight_journal:read(0, infinity),
         [?_assert(element(1, hd(All)) > 1),
          ?_assertEqual(1300, element(1, lists:last(All)))]
     end}.

journal_reads_across_segments_test_() ->
    {setup,
     fun() ->
         Dir = tmp_dir(),
         application:set_env(rabbitmq_access_insight, history_max_disk, 64 * 1024 * 1024),
         {ok, Pid} = rabbit_access_insight_journal:start_link(Dir),
         unlink(Pid),
         Pid
     end,
     fun(Pid) ->
         gen_server:stop(Pid),
         application:unset_env(rabbitmq_access_insight, history_max_disk)
     end,
     fun(_) ->
         Pad = binary:copy(<<"y">>, 8000),      %% 12 MB -> 4 MiB segments
         Now = erlang:system_time(millisecond),
         [ok = rabbit_access_insight_journal:append([?REC(S, Now, t, #{pad => Pad})]) || S <- lists:seq(1, 1500)],
         All = rabbit_access_insight_journal:read(0, infinity),
         #{segments := Segs} = rabbit_access_insight_journal:info(),
         [?_assert(Segs >= 2),
          ?_assertEqual(lists:seq(1, 1500), [element(1, R) || R <- All]),
          ?_assertEqual([1500], [element(1, R) || R <- rabbit_access_insight_journal:read(1499, 5)])]
     end}.

%%----------------------------------------------------------------------------
%% collector, driven by synthetic events on a stand-in rabbit_event manager

collector_test_() ->
    {foreach, fun setup/0, fun cleanup/1,
     [fun logins_and_sessions/1,
      fun refused_after_authentication/1,
      fun confirmed_method_from_access_event/1,
      fun clean_restart_keeps_everything/1,
      fun unclean_restart_replays_and_marks_a_gap/1,
      fun reattaches_after_a_handler_crash/1]}.

setup() ->
    Dir = tmp_dir(),
    application:set_env(rabbitmq_access_insight, history_dir, Dir),
    application:set_env(rabbitmq_access_insight, history_sync_interval, 50),
    {ok, Mgr} = gen_event:start({local, rabbit_event}),
    Pid = start_collector(),
    {Mgr, Pid, Dir}.

cleanup({Mgr, _Pid, _Dir}) ->
    catch gen_server:stop(?C),
    gen_event:stop(Mgr),
    [catch ets:delete(T) || T <- ?AGG_TABLES ++ ?LOCAL_TABLES],
    application:unset_env(rabbitmq_access_insight, history_dir),
    application:unset_env(rabbitmq_access_insight, history_sync_interval).

start_collector() ->
    {ok, Pid} = ?C:start_link(),
    unlink(Pid),
    Pid.

logins_and_sessions(_) ->
    ok_login(<<"alice">>, 1),
    ok_login(<<"alice">>, 2),
    close(2),
    fail_login(<<"alice">>, 3),
    fail_login(<<"ghost">>, 4),
    wait(),
    Alice = user(<<"alice">>),
    [?_assertEqual(2, maps:get(sessions, Alice)),
     ?_assertEqual(1, maps:get(failed, Alice)),
     ?_assertEqual(#{<<"password">> => 2}, maps:get(methods, Alice)),
     ?_assertEqual(#{<<"10.0.0.1">> => 2}, maps:get(sources, Alice)),
     ?_assertEqual(1, maps:get(failed, user(<<"ghost">>))),
     ?_assertEqual(0, maps:get(sessions, user(<<"ghost">>), 0)),
     ?_assertEqual(1, ets:info(?T_SESSION, size))].

refused_after_authentication(_) ->
    notify(user_authentication_success, auth_props(<<"bob">>, 7)),
    notify(connection_closed, [{pid, fake_pid(7)}, {name, conn_name(7)}]),
    wait(),
    Bob = user(<<"bob">>),
    [?_assertEqual(1, maps:get(refused, Bob)),
     ?_assertEqual(0, maps:get(sessions, Bob, 0)),
     ?_assertMatch([{{_, <<"bob">>, <<"10.0.0.1">>, access, _}, #{count := 1}}], ets:tab2list(?T_FAIL))].

confirmed_method_from_access_event(_) ->
    Pid = fake_pid(9),
    notify(user_authentication_success, auth_props(<<"svc">>, 9)),
    notify(access_auth_verified, [{pid, Pid}, {user, <<"svc">>}, {method, token},
                                  {credential, prefixed}, {alg, <<"HS256">>}, {exp, 1893456000}]),
    notify(connection_created, created_props(<<"svc">>, 9)),
    wait(),
    [{_, Sess}] = ets:lookup(?T_SESSION, Pid),
    [?_assertEqual(<<"token">>, maps:get(method, Sess)),
     ?_assertEqual(<<"confirmed">>, maps:get(method_source, Sess)),
     ?_assertMatch(#{alg := <<"HS256">>, exp := 1893456000}, maps:get(verified, Sess))].

clean_restart_keeps_everything(_) ->
    ok_login(<<"carol">>, 11),
    close(11),
    fail_login(<<"carol">>, 12),
    wait(),
    Before = user(<<"carol">>),
    Origin = ?C:origin(),
    Seq = ?C:seq(),
    ok = gen_server:stop(?C),
    [ets:delete(T) || T <- ?AGG_TABLES ++ ?LOCAL_TABLES, ets:info(T) =/= undefined],
    start_collector(),
    [?_assertEqual(Origin, ?C:origin()),
     ?_assertEqual(Seq, ?C:seq()),
     ?_assertEqual(Before, user(<<"carol">>)),
     ?_assertEqual([], gaps())].

unclean_restart_replays_and_marks_a_gap(_) ->
    ok_login(<<"dave">>, 21),
    ok = ?C:snapshot_now(),              %% snapshot holds one session ...
    close(21),
    fail_login(<<"dave">>, 22),          %% ... the journal two more records
    ok = ?C:sync(),
    Before = user(<<"dave">>),
    Seq = ?C:seq(),
    Pid = whereis(?C),
    exit(Pid, kill),
    wait_dead(Pid),
    wait_dead(whereis(rabbit_access_insight_journal)),
    start_collector(),
    After = user(<<"dave">>),
    [?_assertEqual(maps:without([v], Before), maps:without([v], After)),
     ?_assert(?C:seq() > Seq + 1000000),
     ?_assertMatch([#{kind := unclean_shutdown}], gaps())].

reattaches_after_a_handler_crash(_) ->
    {error, {'EXIT', _}} = gen_event:call(rabbit_event, rabbit_access_insight_handler, boom),
    ok = wait_until(fun() -> lists:member(rabbit_access_insight_handler,
                                          gen_event:which_handlers(rabbit_event)) end, 3000),
    ok_login(<<"erin">>, 31),
    wait(),
    [?_assertEqual(1, maps:get(sessions, user(<<"erin">>)))].

%%----------------------------------------------------------------------------

ok_login(User, N) ->
    notify(user_authentication_success, auth_props(User, N)),
    notify(connection_created, created_props(User, N)).

fail_login(User, N) ->
    notify(user_authentication_failure, auth_props(User, N) ++ [{error, "user '" ++ binary_to_list(User) ++ "' - invalid credentials"}]).

close(N) ->
    notify(connection_closed, [{pid, fake_pid(N)}, {name, conn_name(N)}]).

auth_props(User, N) ->
    [{connection_type, network}, {name, User}, {connection_name, conn_name(N)},
     {peer_host, {10, 0, 0, 1}}, {peer_port, 40000 + N}, {protocol, {0, 9, 1}},
     {auth_mechanism, <<"PLAIN">>}, {ssl, false}].

created_props(User, N) ->
    [{pid, fake_pid(N)}, {name, conn_name(N)}, {user, User}, {vhost, <<"/">>},
     {protocol, {0, 9, 1}}, {peer_host, {10, 0, 0, 1}}, {peer_port, 40000 + N},
     {auth_mechanism, <<"PLAIN">>}, {type, network}, {user_provided_name, <<"app">>},
     {connected_at, erlang:system_time(millisecond)}].

conn_name(N) -> iolist_to_binary(io_lib:format("10.0.0.1:~b -> 10.0.0.2:5672", [40000 + N])).

%% A stable, distinct pid per N that stays dead.
fake_pid(N) -> list_to_pid("<0.9999." ++ integer_to_list(N) ++ ">").

notify(Type, Props) ->
    ok = gen_event:sync_notify(rabbit_event, #event{type = Type, props = Props,
                                                    timestamp = erlang:system_time(millisecond)}).

%% The collector handles messages in order, so a call returns after it has
%% processed everything sent before.
wait() -> _ = ?C:seq(), ok.

user(Name) ->
    Origin = ?C:origin(),
    case ets:lookup(?T_USER, {Origin, Name}) of
        [{_, U}] -> U;
        [] -> #{}
    end.

gaps() ->
    [{_, O}] = ets:lookup(?T_ORIGIN, ?C:origin()),
    maps:get(gaps, O, []).

tmp_dir() ->
    Dir = filename:join(["/tmp", lists:flatten(io_lib:format("rai-test-~s-~b-~b",
            [os:getpid(), erlang:system_time(microsecond), erlang:unique_integer([positive])]))]),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    Dir.

wait_dead(undefined) -> ok;
wait_dead(Pid) ->
    Ref = monitor(process, Pid),
    receive {'DOWN', Ref, _, _, _} -> ok after 5000 -> error(timeout) end.

wait_until(Fun, Ms) when Ms =< 0 ->
    Fun() orelse error(timeout),
    ok;
wait_until(Fun, Ms) ->
    case Fun() of
        true  -> ok;
        false -> timer:sleep(50), wait_until(Fun, Ms - 50)
    end.
