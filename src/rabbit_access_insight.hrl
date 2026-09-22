%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 martinx
%% SPDX-License-Identifier: MPL-2.0
%%

-define(APP, rabbitmq_access_insight).

%% Aggregates, one row per origin. An origin is {Node, Epoch}: the node that
%% wrote the data and the life of its history directory. Rows of the local
%% origin are written by the collector only; rows of other origins are
%% replicas written by the sync process only.
-define(T_USER,    rai_user).     %% {{Origin, User}, Map}
-define(T_DAILY,   rai_daily).    %% {{Origin, User, VHost, Day}, Map}
-define(T_FAIL,    rai_fail).     %% {{Origin, User, Source, Stage, Reason}, Map}
-define(T_ORIGIN,  rai_origin).   %% {Origin, Map}
-define(T_TOMB,    rai_tomb).     %% {Origin, ForgottenAtMs}

%% Local-only state, never replicated.
-define(T_SESSION, rai_session).  %% {Pid, Map}: open sessions on this node
-define(T_CTR,     rai_ctr).      %% {Key, Value}: monotonic metric counters
-define(T_STATE,   rai_state).    %% {Key, Value}: counters shared with the handler
-define(T_RECENT,  rai_recent).   %% {Seq, Record}: last closed sessions and failures

-define(AGG_TABLES, [?T_USER, ?T_DAILY, ?T_FAIL, ?T_ORIGIN, ?T_TOMB]).
-define(LOCAL_TABLES, [?T_SESSION, ?T_CTR, ?T_STATE, ?T_RECENT]).
-define(RECENT_MAX, 5000).

%% Journal record: {Seq, TimestampMs, Type, Map}
-define(REC(Seq, Ts, Type, Map), {Seq, Ts, Type, Map}).

-define(SPEC_VERSION, <<"1.0">>).
-define(OTHER, <<"(other)">>).
