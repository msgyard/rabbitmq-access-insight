%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 martinx
%% SPDX-License-Identifier: MPL-2.0
%%
%% Prometheus collector for rabbitmq_prometheus: the access metrics appear
%% on the node's :15692/metrics next to RabbitMQ's own. Registered and
%% deregistered at runtime by rabbit_access_insight_metrics.
-module(rabbit_access_insight_prometheus).
-behaviour(prometheus_collector).

-export([deregister_cleanup/1, collect_mf/2]).

deregister_cleanup(_) -> ok.

collect_mf(_Registry, Callback) ->
    lists:foreach(
      fun({Name, Type, Help, Samples}) ->
              Callback(prometheus_model_helpers:create_mf(list_to_binary(Name), list_to_binary(Help),
                                                          Type, [labels(S) || S <- Samples]))
      end, rabbit_access_insight_metrics:metrics()),
    ok.

labels({L, V}) -> {bin_labels(L), V};
labels({L, B, C, S}) -> {bin_labels(L), B, C, S}.

bin_labels(L) -> [{K, rabbit_access_insight_util:bin(V)} || {K, V} <- L].
