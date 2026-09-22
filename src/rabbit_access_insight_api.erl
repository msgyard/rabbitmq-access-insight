%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 martinx
%% SPDX-License-Identifier: MPL-2.0
%%
%% The HTTP API, independent of how it is served. Both the management
%% extension (/api/access/v1/... on the management port, management login)
%% and the plugin's own listener (/api/access/v1/... plus /metrics) call
%% handle/4 with the path below /api/access/v1.
%%
%%   GET    status                     plugin, capabilities and history of every node
%%   GET    overview                   totals for the cluster
%%   GET    users?state=&search=&sort=&order=&page=&page_size=
%%   GET    users/{name}               profile, daily series, failures, sessions
%%   GET    sessions?user=&limit=      open and recently closed sessions
%%   GET    auth?days=                 login results per day, methods, failures
%%   GET    nodes                      history epochs and replication state
%%   DELETE nodes/{node}               forget a node that left the cluster (administrator)
%%   GET    records?node=&since=&limit=   raw journal records, for export
%%   GET    report | report.csv | report.html   account reconciliation
-module(rabbit_access_insight_api).

-export([handle/4, json/1]).

-type reply() :: {non_neg_integer(), binary(), iodata()}.

-spec handle(binary(), [binary()], [{binary(), binary() | true}], #{admin := boolean()}) -> reply().
handle(Method, Path, Qs, Who) ->
    try route(Method, Path, maps:from_list([{K, V} || {K, V} <- Qs, is_binary(V)]), Who)
    catch
        throw:{bad_request, Msg} -> error_reply(400, Msg);
        C:E:St ->
            logger:warning("rabbitmq_access_insight: API ~ts ~tp failed: ~tp:~tp ~tp", [Method, Path, C, E, St]),
            error_reply(500, <<"internal error; see the broker log">>)
    end.

route(<<"GET">>, [], _Q, W) -> route(<<"GET">>, [<<"status">>], #{}, W);
route(<<"GET">>, [<<"status">>], _Q, _W) ->
    ok(#{spec_version => <<"1.0">>,
         capabilities => rabbit_access_insight_query:capabilities(),
         history => rabbit_access_insight_query:nodes()});
route(<<"GET">>, [<<"overview">>], _Q, _W) ->
    ok(rabbit_access_insight_query:overview());
route(<<"GET">>, [<<"users">>], Q, _W) ->
    ok(rabbit_access_insight_query:users(#{state => maps:get(<<"state">>, Q, undefined),
                                           search => maps:get(<<"search">>, Q, undefined),
                                           sort => maps:get(<<"sort">>, Q, <<"name">>),
                                           order => maps:get(<<"order">>, Q, <<"asc">>),
                                           page => int(Q, <<"page">>, 1),
                                           page_size => int(Q, <<"page_size">>, 100)}));
route(<<"GET">>, [<<"users">>, Name], _Q, _W) ->
    case rabbit_access_insight_query:user(Name) of
        not_found -> error_reply(404, <<"no such user in the definitions or the history">>);
        R -> ok(R)
    end;
route(<<"GET">>, [<<"sessions">>], Q, _W) ->
    ok(rabbit_access_insight_query:sessions(maps:merge(#{limit => int(Q, <<"limit">>, 500)},
                                                       opt(user, Q, <<"user">>))));
route(<<"GET">>, [<<"auth">>], Q, _W) ->
    ok(rabbit_access_insight_query:auth(#{days => int(Q, <<"days">>, 30)}));
route(<<"GET">>, [<<"nodes">>], _Q, _W) ->
    ok(rabbit_access_insight_query:nodes());
route(<<"DELETE">>, [<<"nodes">>, Node], _Q, #{admin := true}) ->
    case rabbit_access_insight_query:forget_node(binary_to_atom(Node, utf8)) of
        {ok, Origins} -> ok(#{forgotten => [#{node => N, epoch => E} || {N, E} <- Origins]});
        {error, Msg} -> error_reply(409, Msg)
    end;
route(<<"DELETE">>, [<<"nodes">>, _], _Q, _W) ->
    error_reply(403, <<"administrator tag required">>);
route(<<"GET">>, [<<"records">>], Q, _W) ->
    Node = case maps:get(<<"node">>, Q, undefined) of
               undefined -> node();
               N -> binary_to_atom(N, utf8)
           end,
    ok(rabbit_access_insight_query:records(Node, int(Q, <<"since">>, 0),
                                           min(10000, int(Q, <<"limit">>, 1000))));
route(<<"GET">>, [<<"report">>], _Q, _W) ->
    ok(#{generated_at => rabbit_access_insight_util:now_ms(), accounts => rabbit_access_insight_query:report()});
route(<<"GET">>, [<<"report.csv">>], _Q, _W) ->
    {200, <<"text/csv; charset=utf-8">>, rabbit_access_insight_report:csv(rabbit_access_insight_query:report())};
route(<<"GET">>, [<<"report.html">>], _Q, _W) ->
    {200, <<"text/html; charset=utf-8">>, rabbit_access_insight_report:html(rabbit_access_insight_query:report())};
route(_, _, _, _) ->
    error_reply(404, <<"not found">>).

ok(Term) -> {200, <<"application/json">>, json(Term)}.

error_reply(Code, Msg) -> {Code, <<"application/json">>, json(#{error => Msg})}.

int(Q, K, Default) ->
    case maps:get(K, Q, undefined) of
        undefined -> Default;
        V -> try binary_to_integer(V) catch _:_ -> throw({bad_request, <<K/binary, " must be an integer">>}) end
    end.

opt(Key, Q, Name) ->
    case maps:get(Name, Q, undefined) of
        undefined -> #{};
        V -> #{Key => V}
    end.

%% JSON of a term built from maps, lists, binaries, numbers and atoms.
json(Term) -> rabbit_json:encode(clean(Term)).

clean(undefined) -> null;
clean(M) when is_map(M) -> maps:from_list([{key(K), clean(V)} || {K, V} <- maps:to_list(M)]);
clean(L) when is_list(L) ->
    case io_lib:printable_unicode_list(L) andalso L =/= [] of
        true  -> unicode:characters_to_binary(L);
        false -> [clean(X) || X <- L]
    end;
clean(T) when is_tuple(T) -> [clean(X) || X <- tuple_to_list(T)];
clean(P) when is_pid(P) -> list_to_binary(pid_to_list(P));
clean(F) when is_function(F) -> null;
clean(X) -> X.

key(K) when is_atom(K); is_binary(K) -> K;
key(K) -> rabbit_access_insight_util:bin(K).
