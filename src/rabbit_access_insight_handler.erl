%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 martinx
%% SPDX-License-Identifier: MPL-2.0
%%
%% Runs inside this node's rabbit_event manager process, so every callback
%% must be cheap and must not block: in-memory updates only, no I/O.
-module(rabbit_access_insight_handler).
-behaviour(gen_event).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([table/0]).
-export([init/1, handle_event/2, handle_call/2, handle_info/2, terminate/2]).

table() -> rabbit_access_insight_counts.

init([]) ->
    {ok, #{}}.

handle_event(#event{type = Type}, State) ->
    _ = ets:update_counter(table(), Type, 1, {Type, 0}),
    {ok, State};
handle_event(_Other, State) ->
    {ok, State}.

handle_call(ping, State) ->
    {ok, pong, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.
