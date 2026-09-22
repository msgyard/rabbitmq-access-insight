%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 martinx
%% SPDX-License-Identifier: MPL-2.0
%%
%% The account reconciliation report, as CSV and as a self-contained HTML page.
-module(rabbit_access_insight_report).

-export([csv/1, html/1]).

-define(COLS, [name, defined, tags, has_password, state, connected, sessions, first_seen,
               last_seen, failed, refused, methods, method_sources, sources]).

csv(Rows) ->
    [<<16#EF, 16#BB, 16#BF>>,     %% BOM, so spreadsheets read UTF-8
     lists:join(<<",">>, [atom_to_binary(C, utf8) || C <- ?COLS]), <<"\r\n">>,
     [[lists:join(<<",">>, [csv_cell(cell(C, R)) || C <- ?COLS]), <<"\r\n">>] || R <- Rows]].

html(Rows) ->
    Cluster = try rabbit_access_insight_util:bin(rabbit_nodes:cluster_name()) catch _:_ -> <<>> end,
    Now = ts(rabbit_access_insight_util:now_ms()),
    Count = fun(Pred) -> integer_to_binary(length([x || R <- Rows, Pred(R)])) end,
    [<<"<!doctype html><html><head><meta charset=\"utf-8\"><title>Account report</title><style>"
       "body{font:14px/1.5 -apple-system,Segoe UI,Helvetica,Arial,sans-serif;margin:24px;color:#1b1f23}"
       "h1{font-size:20px;margin:0 0 4px}p.m{color:#57606a;margin:0 0 16px}"
       "table{border-collapse:collapse;width:100%}th,td{border-bottom:1px solid #d0d7de;padding:5px 8px;text-align:left;vertical-align:top}"
       "th{background:#f6f8fa;font-weight:600}td.n{text-align:right;font-variant-numeric:tabular-nums}"
       ".s{display:inline-block;padding:0 6px;border-radius:9px;font-size:12px;background:#eaeef2}"
       ".never_used,.attempts_only{background:#fff1e5}.dormant{background:#f6f8fa;color:#57606a}"
       ".active_24h,.active_7d{background:#dafbe1}ul.k{display:flex;gap:18px;padding:0;list-style:none}"
       "@media print{body{margin:0}}</style></head><body>">>,
     <<"<h1>Account report</h1><p class=\"m\">">>, esc(Cluster), <<" &middot; generated ">>, Now, <<" UTC</p>">>,
     <<"<ul class=\"k\"><li>Accounts: <b>">>, integer_to_binary(length(Rows)), <<"</b></li>">>,
     <<"<li>Defined: <b>">>, Count(fun(#{defined := D}) -> D end), <<"</b></li>">>,
     <<"<li>Never used: <b>">>, Count(fun(#{state := S}) -> S =:= never_used end), <<"</b></li>">>,
     <<"<li>In use but not defined: <b>">>, Count(fun(#{defined := D, sessions := N}) -> not D andalso N > 0 end), <<"</b></li></ul>">>,
     <<"<table><tr><th>Account</th><th>Defined</th><th>Tags</th><th>State</th><th>Sessions</th>"
       "<th>First seen</th><th>Last seen</th><th>Failed</th><th>Methods</th><th>Top sources</th></tr>">>,
     [[<<"<tr><td>">>, esc(maps:get(name, R)), <<"</td><td>">>, yes(maps:get(defined, R)),
       <<"</td><td>">>, esc(cell(tags, R)), <<"</td><td><span class=\"s ">>, atom_to_binary(maps:get(state, R), utf8), <<"\">">>,
       state_label(maps:get(state, R)), <<"</span></td><td class=\"n\">">>, integer_to_binary(maps:get(sessions, R)),
       <<"</td><td>">>, cell(first_seen, R), <<"</td><td>">>, cell(last_seen, R),
       <<"</td><td class=\"n\">">>, integer_to_binary(maps:get(failed, R) + maps:get(refused, R)),
       <<"</td><td>">>, esc(cell(methods, R)), <<"</td><td>">>, esc(cell(sources, R)), <<"</td></tr>">>]
      || R <- Rows],
     <<"</table></body></html>">>].

cell(C, R) -> fmt(C, maps:get(C, R, null)).

fmt(_, null) -> <<>>;
fmt(C, T) when C =:= first_seen; C =:= last_seen -> ts(T);
fmt(tags, L) -> iolist_to_binary(lists:join(<<" ">>, L));
fmt(sources, L) -> iolist_to_binary(lists:join(<<" ">>, [<<N/binary, "(", (integer_to_binary(K))/binary, ")">>
                                                          || #{name := N, count := K} <- L]));
fmt(_, M) when is_map(M) ->
    iolist_to_binary(lists:join(<<" ">>, [<<(rabbit_access_insight_util:bin(K))/binary, ":", (integer_to_binary(V))/binary>>
                                          || {K, V} <- lists:sort(maps:to_list(M))]));
fmt(_, B) when is_boolean(B) -> atom_to_binary(B, utf8);
fmt(_, A) when is_atom(A) -> atom_to_binary(A, utf8);
fmt(_, I) when is_integer(I) -> integer_to_binary(I);
fmt(_, B) when is_binary(B) -> B.

ts(Ms) ->
    {{Y, Mo, D}, {H, Mi, S}} = calendar:system_time_to_universal_time(Ms, millisecond),
    iolist_to_binary(io_lib:format("~4..0w-~2..0w-~2..0w ~2..0w:~2..0w:~2..0w", [Y, Mo, D, H, Mi, S])).

csv_cell(B) ->
    case binary:match(B, [<<",">>, <<"\"">>, <<"\n">>, <<"\r">>]) of
        nomatch -> guard(B);
        _ -> [<<"\"">>, binary:replace(guard(B), <<"\"">>, <<"\"\"">>, [global]), <<"\"">>]
    end.

%% A cell starting with = + - @ is read as a formula by spreadsheets.
guard(<<C, _/binary>> = B) when C =:= $=; C =:= $+; C =:= $-; C =:= $@ -> <<"'", B/binary>>;
guard(B) -> B.

esc(B) ->
    lists:foldl(fun({F, T}, Acc) -> binary:replace(Acc, F, T, [global]) end, B,
                [{<<"&">>, <<"&amp;">>}, {<<"<">>, <<"&lt;">>}, {<<">">>, <<"&gt;">>}, {<<"\"">>, <<"&quot;">>}]).

yes(true) -> <<"yes">>;
yes(false) -> <<"no">>.

state_label(active_24h) -> <<"active today">>;
state_label(active_7d) -> <<"active this week">>;
state_label(dormant) -> <<"dormant">>;
state_label(never_used) -> <<"never used">>;
state_label(attempts_only) -> <<"failed attempts only">>;
state_label(S) -> atom_to_binary(S, utf8).
