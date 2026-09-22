#!/bin/bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 martinx
# SPDX-License-Identifier: MPL-2.0
#
# Compile and run the EUnit suite.
#   ./run-tests.sh <rabbit_common dir>
#   ./run-tests.sh --rmq-release <version>
set -euo pipefail
RCDIR=$(scripts/rabbit-common.sh "$@")
INCROOT="_build/incroot"; rm -rf "${INCROOT}"; mkdir -p "${INCROOT}"
ln -s "$(cd "${RCDIR}" && pwd)" "${INCROOT}/rabbit_common"
OUT=_build/test; rm -rf "${OUT}"; mkdir -p "${OUT}"
echo ">> OTP $(erl -noshell -eval 'io:format("~s",[erlang:system_info(otp_release)]),halt().'); rabbit_common: ${RCDIR}"
erlc -I "${INCROOT}" -pa "${RCDIR}/ebin" -o "${OUT}" +debug_info src/*.erl
erlc -I "${INCROOT}" -I src -pa "${RCDIR}/ebin" -pa "${OUT}" -o "${OUT}" test/*.erl
MODS=$(cd test && ls *_tests.erl | sed 's/\.erl$//' | paste -sd, -)
# rabbit_json delegates to thoas, which ships alongside rabbit_common
EXTRA_PA=""
for d in "$(dirname "${RCDIR}")"/thoas-*/ebin; do [ -d "$d" ] && EXTRA_PA="${EXTRA_PA} -pa $d"; done
erl -noshell -pa "${OUT}" -pa "${RCDIR}/ebin" ${EXTRA_PA} \
    -eval "case eunit:test([${MODS}], [verbose]) of ok -> halt(0); _ -> halt(1) end."
