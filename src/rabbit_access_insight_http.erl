%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 martinx
%% SPDX-License-Identifier: MPL-2.0
%%
%% The plugin's own HTTP listener, independent of the management plugin:
%%
%%   /                    status page: what is enabled, what is missing, how to enable it
%%   /metrics             access metrics (Prometheus text), no login, like :15692
%%   /api/access/v1/...   the API (rabbit_access_insight_api), HTTP basic auth
%%                        with a monitoring or administrator user
%%
%% It listens on 127.0.0.1:15693 by default (access_insight.http.*).
-module(rabbit_access_insight_http).
-behaviour(gen_server).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([start_link/0, listener_info/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).
%% cowboy handler
-export([init/2]).

-define(CONTEXT, rabbitmq_access_insight).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

listener_info() ->
    #{enabled => rabbit_access_insight_config:get(http_enabled),
      ip => rabbit_access_insight_util:bin(rabbit_access_insight_config:get(http_ip)),
      port => rabbit_access_insight_config:get(http_port)}.

init([]) ->
    process_flag(trap_exit, true),
    case rabbit_access_insight_config:get(http_enabled) of
        true ->
            {ok, IP} = inet:parse_address(rabbit_access_insight_util:bin_to_list(
                                            rabbit_access_insight_config:get(http_ip))),
            Port = rabbit_access_insight_config:get(http_port),
            Dispatch = cowboy_router:compile([{'_', [{"/[...]", ?MODULE, []}]}]),
            {ok, _} = rabbit_web_dispatch:register_context_handler(
                        ?CONTEXT, [{port, Port}, {ip, IP}], "", Dispatch, "Access Insight"),
            logger:info("rabbitmq_access_insight: HTTP listener on ~ts:~b", [inet:ntoa(IP), Port]),
            {ok, #{registered => true}};
        _ ->
            {ok, #{registered => false}}
    end.

handle_call(_, _, S) -> {reply, ok, S}.
handle_cast(_, S) -> {noreply, S}.
handle_info(_, S) -> {noreply, S}.

terminate(_, #{registered := true}) ->
    catch rabbit_web_dispatch:unregister_context(?CONTEXT),
    ok;
terminate(_, _) -> ok.

%%----------------------------------------------------------------------------

init(Req0, State) ->
    Method = cowboy_req:method(Req0),
    Path = cowboy_req:path_info(Req0),
    Req = case Path of
              [] -> reply(200, <<"text/html; charset=utf-8">>, status_page(), Req0);
              [<<"metrics">>] ->
                  reply(200, <<"text/plain; version=0.0.4; charset=utf-8">>,
                        metrics_text(), Req0);
              [<<"api">>, <<"access">>, <<"v1">> | Rest] ->
                  case authenticate(Req0) of
                      {ok, Who} ->
                          {Code, Type, Body} = rabbit_access_insight_api:handle(
                                                 Method, Rest, cowboy_req:parse_qs(Req0), Who),
                          reply(Code, Type, Body, Req0);
                      {error, Msg} ->
                          cowboy_req:reply(401, #{<<"www-authenticate">> => <<"Basic realm=\"RabbitMQ\"">>,
                                                  <<"content-type">> => <<"application/json">>},
                                           rabbit_access_insight_api:json(#{error => Msg}), Req0)
                  end;
              _ -> reply(404, <<"text/plain">>, <<"not found\n">>, Req0)
          end,
    {ok, Req, State}.

reply(Code, Type, Body, Req) ->
    cowboy_req:reply(Code, #{<<"content-type">> => Type, <<"cache-control">> => <<"no-store">>}, Body, Req).

metrics_text() ->
    case lists:member(rabbit_access_insight_metrics:export_mode(), [standalone, both]) of
        true  -> rabbit_access_insight_metrics:render_text(rabbit_access_insight_metrics:metrics());
        false -> <<"# access metrics are served by rabbitmq_prometheus on its /metrics endpoint\n">>
    end.

%% Basic auth against the broker's own authentication chain; a monitoring or
%% administrator tag is required, as for the management API.
authenticate(Req) ->
    case cowboy_req:parse_header(<<"authorization">>, Req) of
        {basic, User, Pass} ->
            case rabbit_access_control:check_user_pass_login(User, Pass) of
                {ok, #user{tags = Tags}} ->
                    Admin = lists:member(administrator, Tags),
                    case Admin orelse lists:member(monitoring, Tags) of
                        true -> {ok, #{admin => Admin}};
                        false -> {error, <<"monitoring or administrator tag required">>}
                    end;
                _ -> {error, <<"invalid credentials">>}
            end;
        _ -> {error, <<"authentication required">>}
    end.

status_page() ->
    Caps = try rabbit_access_insight_query:capabilities() catch _:_ -> #{nodes => []} end,
    Rows = [status_rows(N) || N <- maps:get(nodes, Caps)],
    [<<"<!doctype html><html><head><meta charset=\"utf-8\"><title>Access Insight</title><style>"
       "body{font:14px/1.5 -apple-system,Segoe UI,Helvetica,Arial,sans-serif;margin:32px;color:#1b1f23;max-width:960px}"
       "h1{font-size:20px}table{border-collapse:collapse}td,th{padding:4px 10px;border-bottom:1px solid #d0d7de;text-align:left}"
       "code{background:#f6f8fa;padding:1px 4px;border-radius:4px}.ok{color:#1a7f37}.no{color:#9a6700}</style></head><body>"
       "<h1>Access Insight for RabbitMQ</h1>"
       "<p>API: <code>/api/access/v1/</code> (basic auth, monitoring or administrator) &middot; metrics: <a href=\"metrics\"><code>/metrics</code></a></p>"
       "<table><tr><th>Node</th><th>Capability</th><th>State</th><th>How to enable</th></tr>">>,
     Rows, <<"</table></body></html>">>].

status_rows(#{node := N, plugin := false}) ->
    row(N, <<"rabbitmq_access_insight">>, false, <<"rabbitmq-plugins enable rabbitmq_access_insight">>);
status_rows(C = #{node := N}) ->
    [row(N, <<"Management UI tab and API">>, maps:get(management, C),
         <<"rabbitmq-plugins enable rabbitmq_management">>),
     row(N, <<"Metrics on :15692/metrics">>, maps:get(prometheus, C),
         <<"rabbitmq-plugins enable rabbitmq_prometheus">>),
     row(N, <<"Traffic statistics">>, maps:get(management_agent, C),
         <<"enabled with rabbitmq_management or rabbitmq_prometheus">>),
     row(N, <<"Confirmed authentication methods">>, maps:get(access_events_seen, C) > 0,
         <<"an authentication backend that publishes access_auth_verified events">>)].

row(N, What, Ok, How) ->
    [<<"<tr><td>">>, rabbit_access_insight_util:bin(N), <<"</td><td>">>, What, <<"</td><td class=\"">>,
     case Ok of true -> <<"ok\">enabled">>; _ -> <<"no\">not enabled">> end,
     <<"</td><td>">>, case Ok of true -> <<>>; _ -> [<<"<code>">>, How, <<"</code>">>] end, <<"</td></tr>">>].
