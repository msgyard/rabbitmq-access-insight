#!/usr/bin/env bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 martinx
# SPDX-License-Identifier: MPL-2.0
#
# Start / stop / kill a throwaway single RabbitMQ node for end-to-end tests.
#   node.sh start <work dir> <RABBITMQ_HOME> <plugin .ez> [extra rabbitmq.conf lines...]
#   node.sh stop|kill|restart <work dir>
#   node.sh ctl <work dir> <rabbitmqctl args...>
# The Erlang runtime on PATH must match the plugin build. Ports: AMQP 5701,
# MQTT 1901, STOMP 61701, management 15701, prometheus 15791, plugin API 15793.
set -euo pipefail
CMD=$1 D=$2; shift 2
env_for() {
  export RABBITMQ_NODENAME=rai@localhost RABBITMQ_DIST_PORT=25701
  export RABBITMQ_MNESIA_BASE=$D/data RABBITMQ_LOG_BASE=$D/log RABBITMQ_PID_FILE=$D/pid
  export RABBITMQ_CONFIG_FILE=$D/rabbitmq.conf RABBITMQ_ENABLED_PLUGINS_FILE=$D/enabled_plugins
  export RABBITMQ_PLUGINS_DIR=$D/plugins
  export PATH="$(cat $D/home)/sbin:$PATH"
}
wait_up() { for _ in $(seq 1 60); do rabbitmqctl -q await_startup >/dev/null 2>&1 && return 0; sleep 1; done; echo "node did not start" >&2; exit 1; }
case $CMD in
  start)
    HOMER=$1 EZ=$2; shift 2
    rm -rf "$D"; mkdir -p "$D"/plugins "$D"/data "$D"/log
    echo "$HOMER" > "$D"/home
    for f in "$HOMER"/plugins/*; do ln -s "$f" "$D"/plugins/; done
    cp "$EZ" "$D"/plugins/
    { echo 'listeners.tcp.default = 5701'; echo 'loopback_users = none'
      echo 'mqtt.listeners.tcp.default = 1901'; echo 'stomp.listeners.tcp.1 = 61701'
      echo 'management.tcp.port = 15701'; echo 'prometheus.tcp.port = 15791'
      echo 'access_insight.http.listener.port = 15793'
      for l in "$@"; do echo "$l"; done; } > "$D"/rabbitmq.conf
    echo "[${PLUGINS:-rabbitmq_access_insight,rabbitmq_mqtt,rabbitmq_stomp}]." > "$D"/enabled_plugins
    env_for; nohup rabbitmq-server > "$D"/out.log 2>&1 & wait_up ;;
  stop) env_for; rabbitmqctl -q stop >/dev/null 2>&1 || true ;;
  kill) env_for; kill -9 "$(cat "$D"/pid)"; sleep 2 ;;
  restart) env_for; nohup rabbitmq-server >> "$D"/out.log 2>&1 & wait_up ;;
  ctl) env_for; rabbitmqctl -q "$@" ;;
  plugins) env_for; rabbitmq-plugins -q "$@" ;;
esac
