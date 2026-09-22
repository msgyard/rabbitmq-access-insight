#!/bin/bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 martinx
# SPDX-License-Identifier: MPL-2.0
#
# Build an installable .ez plugin package.
#   ./build-ez.sh <rabbit_common dir>
#   ./build-ez.sh --rmq-release <version>
# Output: _build/<app>-<vsn>-otp<OTP>.ez  (named by the OTP major it was built on)
set -euo pipefail
APP=rabbitmq_access_insight
VSN=$(sed -n 's/.*{vsn, *"\([^"]*\)".*/\1/p' src/${APP}.app.src)
OTP=$(erl -noshell -eval 'io:format("~s",[erlang:system_info(otp_release)]),halt().')
RCDIR=$(scripts/rabbit-common.sh "$@")
# The broker's and other plugins' modules (rabbit, prometheus, the management
# extension behaviour, ...) are only called at runtime; put them on the code
# path so the compiler can check the behaviours.
RABBIT_PA=""
for d in "$(dirname "${RCDIR}")"/*/ebin; do [ -d "$d" ] && RABBIT_PA="${RABBIT_PA} -pa $d"; done

INCROOT="_build/incroot"; rm -rf "${INCROOT}"; mkdir -p "${INCROOT}"
ln -s "$(cd "${RCDIR}" && pwd)" "${INCROOT}/rabbit_common"
echo ">> OTP ${OTP}; rabbit_common: ${RCDIR}"

OUT="_build/${APP}-${VSN}"
rm -rf "${OUT}"; mkdir -p "${OUT}/ebin"
erlc -I "${INCROOT}" -pa "${RCDIR}/ebin" ${RABBIT_PA} -o "${OUT}/ebin" src/*.erl
MODS=$(cd src && ls *.erl | sed 's/\.erl$//' | paste -sd, -)
sed "s/{modules, \[\]}/{modules, [${MODS}]}/" src/${APP}.app.src > "${OUT}/ebin/${APP}.app"
if [ -d priv ]; then cp -R priv "${OUT}/"; fi
EZ="${APP}-${VSN}-otp${OTP}.ez"
( cd _build && rm -f "${EZ}" && zip -qr "${EZ}" "${APP}-${VSN}" )
echo ">> built: _build/${EZ}"
