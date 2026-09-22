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

-define(HANDLER, rabbit_access_insight_handler).

%% A stand-in rabbit_event manager and the real collector, fresh for each test.
collector_test_() ->
    {foreach, fun setup/0, fun cleanup/1,
     [fun counts_events_by_type/1,
      fun ignores_non_events/1,
      fun reattaches_after_a_crash/1,
      fun stays_detached_after_normal_removal/1]}.

setup() ->
    {ok, Mgr} = gen_event:start({local, rabbit_event}),
    {ok, Pid} = rabbit_access_insight_collector:start_link(),
    unlink(Pid),
    {Mgr, Pid}.

cleanup({Mgr, Pid}) ->
    gen_server:stop(Pid),
    gen_event:stop(Mgr).

notify(Type) ->
    ok = gen_event:sync_notify(rabbit_event, #event{type = Type, props = [], timestamp = 0}).

counts_events_by_type(_) ->
    notify(connection_created),
    notify(connection_created),
    notify(user_authentication_success),
    ?_assertEqual([{connection_created, 2}, {user_authentication_success, 1}],
                  rabbit_access_insight_collector:counts()).

ignores_non_events(_) ->
    ok = gen_event:sync_notify(rabbit_event, not_an_event),
    ?_assertEqual([], rabbit_access_insight_collector:counts()).

reattaches_after_a_crash(_) ->
    %% An unexpected call makes the handler crash; the manager drops it.
    {error, {'EXIT', _}} = gen_event:call(rabbit_event, ?HANDLER, boom),
    ?assertNot(attached()),
    ok = wait_until(fun attached/0, 3000),
    notify(connection_closed),
    ?_assertEqual([{connection_closed, 1}], rabbit_access_insight_collector:counts()).

stays_detached_after_normal_removal(_) ->
    ok = gen_event:delete_handler(rabbit_event, ?HANDLER, normal),
    timer:sleep(1500),
    ?_assertNot(attached()).

attached() ->
    lists:member(?HANDLER, gen_event:which_handlers(rabbit_event)).

wait_until(Fun, Ms) when Ms =< 0 ->
    Fun() orelse error(timeout),
    ok;
wait_until(Fun, Ms) ->
    case Fun() of
        true  -> ok;
        false -> timer:sleep(50), wait_until(Fun, Ms - 50)
    end.
