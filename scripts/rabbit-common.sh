#!/bin/bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 martinx
# SPDX-License-Identifier: MPL-2.0
#
# Resolve a rabbit_common directory and echo its path.
#   scripts/rabbit-common.sh <dir>                 use a local plugins/rabbit_common-<vsn>
#   scripts/rabbit-common.sh --rmq-release <ver>   download the generic-unix release and use its copy
set -euo pipefail
if [ "${1:-}" = "--rmq-release" ]; then
  V="${2:?need a RabbitMQ version, e.g. 3.13.7}"
  T="_build/rmq-${V}"
  if [ ! -d "${T}" ]; then
    mkdir -p "${T}"
    URL="https://github.com/rabbitmq/rabbitmq-server/releases/download/v${V}/rabbitmq-server-generic-unix-${V}.tar.xz"
    echo ">> downloading ${URL}" >&2
    curl -fsSL "${URL}" | tar -xJ -C "${T}"
  fi
  ls -d ${T}/rabbitmq_server-*/plugins/rabbit_common-*
else
  echo "${1:?usage: <rabbit_common dir> | --rmq-release <version>}"
fi
