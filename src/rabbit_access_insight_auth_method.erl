%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 martinx
%% SPDX-License-Identifier: MPL-2.0
%%
%% Which authentication method a login used.
%%
%% Confirmed when the authentication backend published an access_auth_verified
%% event for the connection (the access event convention); otherwise inferred
%% from what the broker knows, or unknown. The rules, in order:
%%
%%   1. access_auth_verified for the connection    -> its method, confirmed
%%   2. SASL mechanism EXTERNAL                    -> certificate, inferred
%%   3. only the internal backend is configured    -> password, inferred
%%   4. the user has no password in the internal
%%      database, or is not in it at all            -> other, inferred
%%   5. otherwise                                  -> unknown
-module(rabbit_access_insight_auth_method).

-export([resolve/3, only_internal/1]).

-spec resolve(map() | undefined, term(), binary()) -> {binary(), binary()}.
resolve(#{method := Method}, _Mechanism, _User) ->
    {Method, <<"confirmed">>};
resolve(_, <<"EXTERNAL">>, _User) ->
    {<<"certificate">>, <<"inferred">>};
resolve(_, _Mechanism, User) ->
    case only_internal(application:get_env(rabbit, auth_backends, [rabbit_auth_backend_internal])) of
        true -> {<<"password">>, <<"inferred">>};
        false ->
            case has_password(User) of
                false -> {<<"other">>, <<"inferred">>};
                _     -> {<<"unknown">>, <<"none">>}
            end
    end.

only_internal(Backends) when is_list(Backends), Backends =/= [] ->
    lists:all(fun internal/1, Backends);
only_internal(_) -> false.

internal({AuthN, _AuthZ}) -> internal(AuthN);
internal(rabbit_auth_backend_internal) -> true;
internal(rabbit_auth_backend_internal_loopback) -> true;
internal(internal) -> true;
internal(_) -> false.

%% true | false | unknown
has_password(User) ->
    try rabbit_auth_backend_internal:lookup_user(User) of
        {ok, U} ->
            case internal_user:get_password_hash(U) of
                <<>>      -> false;
                undefined -> false;
                _         -> true
            end;
        _ -> false
    catch _:_ -> unknown
    end.
