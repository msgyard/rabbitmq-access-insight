%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 martinx
%% SPDX-License-Identifier: MPL-2.0
%%
%% Owns the event handler on this node's rabbit_event manager.
%%
%% The handler is added with gen_event:add_sup_handler/3, so it is tied to
%% this process: if the handler crashes, the manager removes it and tells us
%% with a gen_event_EXIT message, and we attach it again. Nothing the handler
%% does can take the event manager -- or the broker -- down with it.
-module(rabbit_access_insight_collector).
-behaviour(gen_server).

-include_lib("rabbit_common/include/logging.hrl").
-include_lib("kernel/include/logger.hrl").

-export([start_link/0, counts/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(HANDLER, rabbit_access_insight_handler).
-define(REATTACH_MS, 1000).
-define(LOG_META, #{domain => ?RMQLOG_DOMAIN_GLOBAL}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% Events seen on this node since the collector started, by event type.
-spec counts() -> [{atom(), non_neg_integer()}].
counts() ->
    lists:sort(ets:tab2list(?HANDLER:table())).

init([]) ->
    _ = ets:new(?HANDLER:table(), [named_table, public, set, {write_concurrency, true}]),
    ok = attach(),
    {ok, #{}}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({gen_event_EXIT, ?HANDLER, normal}, State) ->
    {noreply, State};
handle_info({gen_event_EXIT, ?HANDLER, Reason}, State) ->
    ?LOG_WARNING("rabbitmq_access_insight: event handler exited (~tp); re-attaching",
                 [Reason], ?LOG_META),
    erlang:send_after(?REATTACH_MS, self(), attach),
    {noreply, State};
handle_info(attach, State) ->
    ok = attach(),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

attach() ->
    gen_event:add_sup_handler(rabbit_event, ?HANDLER, []).
