%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 martinx
%% SPDX-License-Identifier: MPL-2.0
%%
%% Runs inside this node's rabbit_event manager process, so every callback
%% must be cheap and must not block: it only forwards the events it needs to
%% the collector, as a plain message.
%%
%% If the collector falls behind by more than max_queue messages, events are
%% dropped instead of queued, and counted; the collector records the loss as
%% an overload gap. The broker's event distribution is never slowed down.
-module(rabbit_access_insight_handler).
-behaviour(gen_event).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("rabbit_access_insight.hrl").

-export([init/1, handle_event/2, handle_call/2, handle_info/2, terminate/2]).

-define(COLLECTOR, rabbit_access_insight_collector).

init([]) ->
    {ok, #{max_queue => rabbit_access_insight_config:get(max_queue)}}.

handle_event(#event{type = Type, props = Props, timestamp = Ts}, State = #{max_queue := Max})
  when Type =:= user_authentication_success;
       Type =:= user_authentication_failure;
       Type =:= connection_created;
       Type =:= connection_closed;
       Type =:= access_auth_verified ->
    case whereis(?COLLECTOR) of
        undefined -> drop(Ts);
        Pid ->
            case erlang:process_info(Pid, message_queue_len) of
                {message_queue_len, N} when N < Max -> Pid ! {event, Type, Props, Ts};
                _ -> drop(Ts)
            end
    end,
    {ok, State};
handle_event(_Other, State) ->
    {ok, State}.

handle_call(ping, State) ->
    {ok, pong, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

drop(Ts) ->
    try
        case ets:update_counter(?T_STATE, dropped, {2, 1}, {dropped, 0, Ts}) of
            _ -> ok
        end
    catch _:_ -> ok
    end.
