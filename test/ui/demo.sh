#!/usr/bin/env bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 martinx
# SPDX-License-Identifier: MPL-2.0
#
# A three-node demo cluster with 30 days of fictional history, for the
# screenshots. Management UI on :15701 (user demo / demo).
#   demo.sh <work dir> <RABBITMQ_HOME> <plugin .ez>
set -euo pipefail
W=$1 HOMER=$2 EZ=$3
HERE=$(cd "$(dirname "$0")" && pwd); E2E=$HERE/../e2e
export PLUGINS=rabbitmq_access_insight,rabbitmq_management,rabbitmq_prometheus
CONF=('access_insight.replication.interval = 2000')
n()   { NODE_INDEX=$1 "$E2E/node.sh" "$2" "$W/n$1" "${@:3}"; }
ctl() { local i=$1; shift; NODE_INDEX=$i "$E2E/node.sh" ctl "$W/n$i" "$@"; }
n 0 start "$HOMER" "$EZ" "${CONF[@]}"
for i in 1 2; do
  n "$i" start "$HOMER" "$EZ" "${CONF[@]}"
  ctl "$i" stop_app >/dev/null; ctl "$i" join_cluster rai@localhost >/dev/null; ctl "$i" start_app >/dev/null
done
ctl 0 set_cluster_name demo-cluster >/dev/null
ctl 0 add_user demo demo >/dev/null; ctl 0 set_user_tags demo administrator >/dev/null
ctl 0 add_vhost iot >/dev/null
for u in orders-service billing-api inventory-sync reporting-etl iot-gateway notification-worker \
         payment-callback ops-monitor legacy-batch dev-sandbox audit-reader partner-feed; do
  ctl 0 add_user "$u" "$(head -c 9 /dev/urandom | base64)" >/dev/null
done
ctl 0 set_user_tags ops-monitor monitoring >/dev/null
for u in billing-api inventory-sync payment-callback; do ctl 0 clear_password "$u" >/dev/null; done
python3 "$HERE/demo_events.py" "$W/events.term" rai@localhost rai1@localhost rai2@localhost >/dev/null
ctl 0 eval "{ok, Evs} = file:consult(\"$W/events.term\"),
  Fix = fun(P) -> [{K, case V of {fakepid, N} -> list_to_pid(\"<0.\" ++ integer_to_list(N) ++ \".0>\"); _ -> V end} || {K, V} <- P] end,
  [{rabbit_access_insight_collector, Node} ! {event, T, Fix(P), Ts} || {Node, T, P, Ts} <- Evs], length(Evs)." >/dev/null
sleep 8
echo "demo cluster ready: http://127.0.0.1:15701/ (demo / demo)"
