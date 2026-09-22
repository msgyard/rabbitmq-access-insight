#!/usr/bin/env bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 martinx
# SPDX-License-Identifier: MPL-2.0
#
# Three-node cluster test of replicated account history (design 4.3.4):
#   1 no double counting    2 wiped data directory    3 stopped node
#   4 isolated replication  5 forget a node that left the cluster
#
#   cluster_e2e.sh <work dir> <RABBITMQ_HOME> <plugin .ez> <python with pika>
# The Erlang runtime on PATH must match the plugin build.
set -uo pipefail
W=$1 HOMER=$2 EZ=$3 PY=$4
HERE=$(cd "$(dirname "$0")" && pwd)
export PLUGINS=rabbitmq_access_insight,rabbitmq_management
CONF=('access_insight.replication.interval = 2000' 'access_insight.history.snapshot_interval = 3000')
FAIL=0
n()   { NODE_INDEX=$1 "$HERE/node.sh" "$2" "$W/n$1" "${@:3}"; }
ctl() { local i=$1; shift; NODE_INDEX=$i "$HERE/node.sh" ctl "$W/n$i" "$@"; }
api() { curl -s -u guest:guest "http://127.0.0.1:$((15701 + $1))/api/access/v1/$2"; }
ok()  { if [ "$2" = "$3" ]; then echo "  ✅ $1"; else echo "  ❌ $1 (got: $2, want: $3)"; FAIL=1; fi; }
node_name() { [ "$1" = 0 ] && echo rai@localhost || echo "rai$1@localhost"; }
field() { "$PY" -c "import json,sys; d=json.load(sys.stdin); print($1)"; }
user_on() { api "$1" "users/$2" | field "d.get('user',{}).get('$3',0)"; }
login() { # login <node index> <user> <password> [count]
  "$PY" - "$@" <<'PY'
import pika, sys
i, u, p = int(sys.argv[1]), sys.argv[2], sys.argv[3]
for _ in range(int(sys.argv[4]) if len(sys.argv) > 4 else 1):
    try:
        c = pika.BlockingConnection(pika.ConnectionParameters('127.0.0.1', 5701 + i, '/', pika.PlainCredentials(u, p), socket_timeout=5))
        c.channel(); c.close()
    except Exception:
        pass
PY
}
local_sessions() { # sessions of user in node i's own contribution only
  ctl "$1" eval "O = rabbit_access_insight_collector:origin(), case ets:lookup(rai_user, {O, <<\"$2\">>}) of [{_, M}] -> maps:get(sessions, M, 0); [] -> 0 end." | tail -1
}
digest() { ctl "$1" eval "lists:sort(maps:to_list(maps:get(origins, rabbit_access_insight_sync:digest())))." | tr -d ' \n'; }
settle() { sleep 7; }

echo "== starting three nodes"
n 0 start "$HOMER" "$EZ" "${CONF[@]}"
for i in 1 2; do
  n "$i" start "$HOMER" "$EZ" "${CONF[@]}"
  ctl "$i" stop_app >/dev/null; ctl "$i" join_cluster rai@localhost >/dev/null; ctl "$i" start_app >/dev/null
done
ctl 0 add_user alice pw >/dev/null; ctl 0 set_permissions alice '.*' '.*' '.*' >/dev/null
ctl 0 add_user bob pw >/dev/null;   ctl 0 set_permissions bob '.*' '.*' '.*' >/dev/null
echo "   cluster: $(ctl 0 eval 'rabbit_nodes:list_running().' | tr -d '\n ')"

echo "== 1. logins spread over the nodes, queried from each node"
login 0 alice pw 3; login 1 alice pw 2; login 2 alice pw 1; login 2 bob pw 2
login 1 alice wrong 1; login 0 carol x 1
settle
for i in 0 1 2; do
  ok "node $i sees alice: 6 sessions" "$(user_on $i alice sessions)" 6
  ok "node $i sees alice: 1 failed"   "$(user_on $i alice failed)" 1
  ok "node $i sees bob: 2 sessions"   "$(user_on $i bob sessions)" 2
done
SUM=$(( $(local_sessions 0 alice) + $(local_sessions 1 alice) + $(local_sessions 2 alice) ))
ok "cluster total equals the sum of the three nodes' own counts" "$SUM" 6
D0=$(digest 0); ok "node 1 holds the same versions as node 0" "$(digest 1)" "$D0"; ok "node 2 holds the same versions as node 0" "$(digest 2)" "$D0"

echo "== 2. node 2 loses its history directory"
n 2 stop
rm -rf "$W"/n2/data/*/access_insight
n 2 restart; settle; settle
for i in 0 2; do
  ok "node $i still sees alice: 6 sessions" "$(user_on $i alice sessions)" 6
  ok "node $i still sees bob: 2 sessions"   "$(user_on $i bob sessions)" 2
done
ok "node 2 holds four contributions (its old and new epoch)" "$(api 2 nodes | field "len(d['origins'])")" 4

echo "== 3. node 1 is stopped"
n 1 stop; sleep 2
ok "node 0 still sees alice: 6 sessions" "$(user_on 0 alice sessions)" 6
ok "node 1 is listed as incomplete" "$(api 0 users | field "d['incomplete']")" "['rai1@localhost']"
ok "node 1's contribution is marked not running" \
   "$(api 0 nodes | field "sorted(set(o['state'] for o in d['origins'] if o['node']=='rai1@localhost'))")" "['not_running']"
n 1 restart; settle

echo "== 4. replication off while both sides take logins, then on again"
for i in 0 1 2; do ctl $i eval 'application:set_env(rabbitmq_access_insight, replication_enabled, false).' >/dev/null; done
login 0 alice pw 2; login 1 alice pw 3
sleep 4
ok "node 0 does not see node 1's new logins while isolated" "$(user_on 0 alice sessions)" 8
for i in 0 1 2; do ctl $i eval 'application:set_env(rabbitmq_access_insight, replication_enabled, true).' >/dev/null; done
settle
for i in 0 1 2; do ok "node $i converges to 11 sessions" "$(user_on $i alice sessions)" 11; done
D0=$(digest 0); ok "all nodes hold the same versions" "$(digest 1)$(digest 2)" "$D0$D0"

echo "== 5. node 2 leaves the cluster and is forgotten"
n 2 stop
ctl 0 forget_cluster_node rai2@localhost >/dev/null
CODE=$(curl -s -o /dev/null -w '%{http_code}' -u guest:guest -X DELETE "http://127.0.0.1:15701/api/access/v1/nodes/rai2@localhost")
ok "DELETE nodes/rai2 answered 200" "$CODE" 200
sleep 3
for i in 0 1; do
  ok "node $i: alice back to 10 sessions (node 2 had 1)" "$(user_on $i alice sessions)" 10
  ok "node $i: bob gone (his 2 sessions were on node 2)" "$(user_on $i bob sessions)" 0
done
n 1 stop; n 0 stop
# a two-node cluster needs both members to boot: start them together
n 0 restart & n 1 restart; wait; settle
for i in 0 1; do
  ok "after restarting both nodes, node $i still has no rai2 contribution" \
     "$(api $i nodes | field "[o for o in d['origins'] if o['node']=='rai2@localhost']")" "[]"
done
n 0 stop; n 1 stop
exit $FAIL
