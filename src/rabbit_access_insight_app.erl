%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 martinx
%% SPDX-License-Identifier: MPL-2.0
%%
-module(rabbit_access_insight_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    rabbit_access_insight_sup:start_link().

stop(_State) ->
    ok.
