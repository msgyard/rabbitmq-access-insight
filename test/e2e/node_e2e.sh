#!/usr/bin/env bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 martinx
# SPDX-License-Identifier: MPL-2.0
#
# Single-node test on a real broker:
#   * enabled on a running node, it picks up the connection already open
#   * logins over AMQP 0-9-1, AMQP 1.0, MQTT and STOMP, failed and refused
#   * clean restart and kill -9 keep the history; the crash is marked
#
#   node_e2e.sh <work dir> <RABBITMQ_HOME> <plugin .ez> <python with pika>
# The Erlang runtime on PATH must match the plugin build.
set -uo pipefail
D=$1 HOMER=$2 EZ=$3 PY=$4
HERE=$(cd "$(dirname "$0")" && pwd)
FAIL=0
N()   { "$HERE/node.sh" "$@"; }
ctl() { N ctl "$D" "$@"; }
ok()  { if [ "$2" = "$3" ]; then echo "  ✅ $1"; else echo "  ❌ $1 (got: $2, want: $3)"; FAIL=1; fi; }
V=$(basename "$HOMER" | sed 's/rabbitmq_server-//')
case $V in 3.*) AMQP10=rabbitmq_amqp1_0, ;; *) AMQP10= ;; esac
export PLUGINS="${AMQP10}rabbitmq_mqtt,rabbitmq_stomp,rabbitmq_management"
N start "$D" "$HOMER" "$EZ"
ctl add_user alice pw >/dev/null; ctl set_permissions alice '.*' '.*' '.*' >/dev/null
ctl add_vhost other >/dev/null; ctl add_user bob pw >/dev/null; ctl set_permissions -p other bob '.*' '.*' '.*' >/dev/null
ctl add_user mon pw >/dev/null; ctl set_user_tags mon monitoring >/dev/null
api() { curl -s -u mon:pw "http://127.0.0.1:15701/api/access/v1/$1"; }
jq()  { "$PY" -c "import json,sys; d=json.load(sys.stdin); print($1)"; }
echo "RabbitMQ $(ctl version | tail -1)"

echo "== enabled on a running node"
rm -f "$D/release"
"$PY" "$HERE/collector_e2e.py" --hold alice pw "$D/release" &
HOLD=$!
sleep 3
N plugins "$D" enable rabbitmq_access_insight >/dev/null; sleep 3
ok "the connection opened before the plugin is an open session" "$(api sessions | jq "len(d['open'])")" 1
ok "... recorded as picked up, for alice" "$(api sessions | jq "[(s['user'], s['bootstrap']) for s in d['open']]")" "[('alice', True)]"
touch "$D/release"; wait $HOLD; sleep 1
ok "closing it records the session" "$(api users/alice | jq "d['user']['sessions']")" 1

echo "== logins over four protocols"
"$PY" "$HERE/collector_e2e.py" --protocols alice pw >/dev/null
# AMQP 1.0 with RabbitMQ's own Erlang client
PA=""; for e in "$HOMER"/plugins/*/ebin; do PA="$PA -pa $e"; done
A10='application:ensure_all_started(amqp10_client),
  Try = fun(U, P) ->
    {ok, C} = amqp10_client:open_connection(#{address => "127.0.0.1", port => 5701,
                sasl => {plain, U, P}, hostname => <<"vhost:/">>}),
    R = receive {amqp10_event, {connection, C, opened}} -> ok;
                {amqp10_event, {connection, C, {closed, _}}} -> refused
        after 5000 -> timeout end,
    catch amqp10_client:close_connection(C), R end,
  io:format("~p ~p~n", [Try(<<"alice">>, <<"pw">>), Try(<<"alice">>, <<"nope">>)]), halt().'
ok "AMQP 1.0: one login accepted, one refused" "$(erl -noshell $PA -eval "$A10" 2>/dev/null | tail -1)" "ok refused"
sleep 2
U=$(api users/alice)
ok "alice: 1 + 3 AMQP 0-9-1 + 1 AMQP 1.0 + 1 MQTT + 1 STOMP sessions" "$(echo "$U" | jq "d['user']['sessions']")" 7
ok "alice: failed 2 AMQP 0-9-1 + 1 AMQP 1.0 + 1 MQTT + 1 STOMP" "$(echo "$U" | jq "d['user']['failed']")" 5
ok "alice: protocols" "$(echo "$U" | jq "sorted(d['user']['protocols'].items())")" \
   "[('AMQP 0-9-1', 4), ('AMQP 1.0', 1), ('MQTT 3.1.1', 1), ('STOMP 1.2', 1)]"
ok "ghost: failed only" "$(api users/ghost | jq "(d['user']['failed'], d['user']['sessions'], d['user']['state'])")" "(1, 0, 'attempts_only')"
ok "bob: refused after authentication" "$(api users/bob | jq "(d['user']['refused'], d['user']['sessions'])")" "(1, 0)"
BEFORE=$(api users | jq "sorted((u['name'], u['sessions'], u['failed'], u['refused']) for u in d['items'])")
SEQ=$(ctl eval 'rabbit_access_insight_collector:seq().' | tail -1)

echo "== clean restart"
N stop "$D"; N restart "$D"
ok "users unchanged" "$(api users | jq "sorted((u['name'], u['sessions'], u['failed'], u['refused']) for u in d['items'])")" "$BEFORE"
ok "sequence unchanged" "$(ctl eval 'rabbit_access_insight_collector:seq().' | tail -1)" "$SEQ"
ok "no gap" "$(api nodes | jq "[g['kind'] for o in d['origins'] for g in o['gaps']]")" "[]"

echo "== kill -9"
N kill "$D"; N restart "$D"
ok "users unchanged" "$(api users | jq "sorted((u['name'], u['sessions'], u['failed'], u['refused']) for u in d['items'])")" "$BEFORE"
ok "unclean stop marked" "$(api nodes | jq "[g['kind'] for o in d['origins'] for g in o['gaps']]")" "['unclean_shutdown']"
N stop "$D"
exit $FAIL
