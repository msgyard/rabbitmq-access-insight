%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 martinx
%% SPDX-License-Identifier: MPL-2.0
%%
%% Management plugin extension: the API under the management port's /api
%% (management login and permissions) and the "Access" tab of the UI.
%% Found by rabbitmq_management through the rabbit_mgmt_extension behaviour;
%% nothing happens when the management plugin is not enabled.
-module(rabbit_access_insight_mgmt).
-behaviour(rabbit_mgmt_extension).

-export([dispatcher/0, web_ui/0]).
%% cowboy_rest
-export([init/2, allowed_methods/2, is_authorized/2, content_types_provided/2,
         resource_exists/2, delete_resource/2, to_json/2]).

%% Same record as rabbitmq_web_dispatch's #context{} (unchanged 3.12 - 4.3).
-record(context, {user, password = none, impl}).

dispatcher() ->
    [{"/access/v1/[...]", ?MODULE, []}].

web_ui() ->
    [{javascript, <<"access.js">>}].

init(Req, _) ->
    {cowboy_rest, rabbit_mgmt_cors:set_headers(Req, ?MODULE), #context{}}.

allowed_methods(Req, C) ->
    {[<<"GET">>, <<"DELETE">>, <<"OPTIONS">>, <<"HEAD">>], Req, C}.

is_authorized(Req, C) ->
    case cowboy_req:method(Req) of
        <<"DELETE">> -> rabbit_mgmt_util:is_authorized_admin(Req, C);
        _ -> rabbit_mgmt_util:is_authorized_monitor(Req, C)
    end.

content_types_provided(Req, C) ->
    {[{<<"application/json">>, to_json}, {<<"text/csv">>, to_json}, {<<"text/html">>, to_json}], Req, C}.

resource_exists(Req, C) ->
    {true, Req, C}.

to_json(Req, C) ->
    {Code, Type, Body} = call(<<"GET">>, Req, C),
    Req1 = cowboy_req:reply(Code, #{<<"content-type">> => Type, <<"cache-control">> => <<"no-store">>},
                            Body, Req),
    {stop, Req1, C}.

delete_resource(Req, C) ->
    {Code, Type, Body} = call(<<"DELETE">>, Req, C),
    Req1 = cowboy_req:reply(Code, #{<<"content-type">> => Type}, Body, Req),
    {stop, Req1, C}.

call(Method, Req, #context{user = User}) ->
    Tags = try element(3, User) catch _:_ -> [] end,   %% #user{username, tags, ...}
    rabbit_access_insight_api:handle(Method, cowboy_req:path_info(Req), cowboy_req:parse_qs(Req),
                                     #{admin => lists:member(administrator, Tags)}).
