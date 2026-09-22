%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 martinx
%% SPDX-License-Identifier: MPL-2.0
%%
%% Small, pure helpers shared by the other modules.
-module(rabbit_access_insight_util).

-export([now_ms/0, day/1, day_start/1, days_between/2,
         ip/1, bin/1, bin/2, pid_bin/1, protocol/1,
         client_name/1, conn_peer/1, vm_id/0, prop/2, prop/3]).

-define(MS_PER_DAY, 86400000).

now_ms() -> erlang:system_time(millisecond).

%% UTC calendar day of a millisecond timestamp, as <<"YYYY-MM-DD">>.
-spec day(integer()) -> binary().
day(Ms) ->
    {{Y, M, D}, _} = calendar:system_time_to_universal_time(Ms, millisecond),
    iolist_to_binary(io_lib:format("~4..0w-~2..0w-~2..0w", [Y, M, D])).

%% Millisecond timestamp of 00:00 UTC of the day containing Ms.
day_start(Ms) -> Ms - (Ms rem ?MS_PER_DAY).

%% [{Day, MsWithinThatDay}] covering [From, To), split at UTC midnight.
-spec days_between(integer(), integer()) -> [{binary(), non_neg_integer()}].
days_between(From, To) when To =< From -> [];
days_between(From, To) ->
    Next = day_start(From) + ?MS_PER_DAY,
    case To =< Next of
        true  -> [{day(From), To - From}];
        false -> [{day(From), Next - From} | days_between(Next, To)]
    end.

%% IPv4-mapped IPv6 addresses are shown as IPv4.
-spec ip(term()) -> binary().
ip({0, 0, 0, 0, 0, 16#ffff, A, B}) ->
    ip({A bsr 8, A band 16#ff, B bsr 8, B band 16#ff});
ip(Addr) when is_tuple(Addr) ->
    case inet:ntoa(Addr) of
        {error, _} -> <<"unknown">>;
        S          -> list_to_binary(S)
    end;
ip(Addr) when is_binary(Addr) -> Addr;
ip(Addr) when is_list(Addr) -> bin(Addr);
ip(_) -> <<"unknown">>.

bin(X) -> bin(X, 256).

%% A printable binary of at most Max bytes, cut on a UTF-8 boundary.
bin(B, Max) when is_binary(B) -> cut(B, Max);
bin(L, Max) when is_list(L) ->
    try unicode:characters_to_binary(L) of
        B when is_binary(B) -> cut(B, Max);
        _ -> cut(iolist_to_binary(io_lib:format("~tp", [L])), Max)
    catch _:_ -> cut(iolist_to_binary(io_lib:format("~tp", [L])), Max)
    end;
bin(A, Max) when is_atom(A) -> cut(atom_to_binary(A, utf8), Max);
bin(I, _) when is_integer(I) -> integer_to_binary(I);
bin(T, Max) -> cut(iolist_to_binary(io_lib:format("~tp", [T])), Max).

cut(B, Max) when byte_size(B) =< Max -> B;
cut(B, Max) ->
    case unicode:characters_to_list(binary:part(B, 0, Max)) of
        {incomplete, L, _} -> unicode:characters_to_binary(L);
        {error, L, _}      -> unicode:characters_to_binary(L);
        L                  -> unicode:characters_to_binary(L)
    end.

pid_bin(Pid) when is_pid(Pid) -> list_to_binary(pid_to_list(Pid));
pid_bin(Other) -> bin(Other).

%% {0,9,1} -> <<"AMQP 0-9-1">>, {'MQTT',{3,1,1}} -> <<"MQTT 3.1.1">>, ...
-spec protocol(term()) -> binary().
protocol({0, 9, 1})              -> <<"AMQP 0-9-1">>;
protocol({1, 0})                 -> <<"AMQP 1.0">>;
protocol({'AMQP', {1, 0}})       -> <<"AMQP 1.0">>;
protocol({Name, Vsn}) when is_atom(Name) -> <<(bin(Name))/binary, " ", (vsn(Vsn))/binary>>;
protocol(Name) when is_atom(Name), Name =/= undefined -> bin(Name);
protocol(Bin) when is_binary(Bin) -> Bin;
protocol(_) -> <<"unknown">>.

vsn(T) when is_tuple(T) ->
    iolist_to_binary(lists:join(".", [integer_to_list(I) || I <- tuple_to_list(T), is_integer(I)]));
vsn(L) when is_list(L) -> bin(L);
vsn(B) when is_binary(B) -> B;
vsn(_) -> <<>>.

%% What the client calls itself: the connection name it chose, else the
%% product and version it reports, else the MQTT client id.
-spec client_name(list()) -> binary().
client_name(Props) ->
    CP = prop(client_properties, Props, []),
    case {prop(user_provided_name, Props), table(<<"connection_name">>, CP),
          table(<<"product">>, CP), table(<<"version">>, CP), table(client_id, CP)} of
        {N, _, _, _, _} when is_binary(N), N =/= <<>> -> bin(N, 128);
        {_, N, _, _, _} when is_binary(N), N =/= <<>> -> bin(N, 128);
        {_, _, P, V, _} when is_binary(P), is_binary(V) -> bin(<<P/binary, " ", V/binary>>, 128);
        {_, _, P, _, _} when is_binary(P) -> bin(P, 128);
        {_, _, _, _, C} when is_binary(C) -> bin(C, 128);
        _ -> <<"unknown">>
    end.

table(Key, CP) when is_list(CP) ->
    case lists:keyfind(Key, 1, CP) of
        {_, _, V} when is_binary(V) -> V;
        {_, V} when is_binary(V) -> V;
        _ -> undefined
    end;
table(_, _) -> undefined.

%% Peer address of a "1.2.3.4:5678 -> 5.6.7.8:5672" connection name.
-spec conn_peer(term()) -> binary().
conn_peer(Name) when is_binary(Name) ->
    case binary:split(Name, <<" -> ">>) of
        [Peer, _] ->
            case string:split(Peer, ":", trailing) of
                [Host, _Port] -> Host;
                _ -> Peer
            end;
        _ -> <<"unknown">>
    end;
conn_peer(_) -> <<"unknown">>.

%% Identifies this run of the Erlang VM, so pids recorded before a restart
%% are never mistaken for live ones.
vm_id() -> {list_to_binary(os:getpid()), erlang:system_info(creation)}.

prop(K, L) -> prop(K, L, undefined).
prop(K, L, Default) when is_list(L) ->
    case lists:keyfind(K, 1, L) of
        {K, V} -> V;
        _      -> Default
    end;
prop(_, _, Default) -> Default.
