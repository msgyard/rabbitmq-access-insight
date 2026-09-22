%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 martinx
%% SPDX-License-Identifier: MPL-2.0
%%
-module(rabbit_access_insight_config).

-include("rabbit_access_insight.hrl").

-export([get/1, history_dir/0, all/0]).
-compile({no_auto_import, [get/1]}).

defaults() ->
    #{history_max_disk      => 512 * 1024 * 1024,
      history_log_days      => 30,
      history_rollup_days   => 400,
      history_sync_interval => 1000,
      snapshot_interval     => 300000,
      replication_enabled   => true,
      replication_interval  => 60000,
      metrics_export        => auto,
      metrics_per_user      => true,
      http_enabled          => true,
      http_ip               => "127.0.0.1",
      http_port             => 15693,
      max_queue             => 50000,
      pending_ttl           => 120000,
      max_set               => 64,
      max_users             => 100000,
      max_reason_bytes      => 160}.

-spec get(atom()) -> term().
get(Key) ->
    application:get_env(?APP, Key, maps:get(Key, defaults())).

all() ->
    maps:map(fun(K, _) -> get(K) end, defaults()).

%% <data dir>/access_insight unless configured: it moves together with the
%% node's data directory.
-spec history_dir() -> file:filename().
history_dir() ->
    case application:get_env(?APP, history_dir) of
        {ok, Dir} when Dir =/= "", Dir =/= undefined -> Dir;
        _ -> filename:join(rabbit:data_dir(), "access_insight")
    end.
