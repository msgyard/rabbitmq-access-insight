# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 martinx
# SPDX-License-Identifier: MPL-2.0
"""Drive logins over AMQP 0-9-1, MQTT and STOMP against a node started by
node.sh, and print what the node was asked to do, as JSON, so the caller can
compare it with what the plugin recorded. Needs `pika`.

    python collector_e2e.py <password> <release file>

Users are created by the caller. One AMQP connection is kept open until the
release file appears.
"""
import json, os, socket, struct, sys, time
import pika

PW, RELEASE = sys.argv[1], sys.argv[2]
AMQP, MQTT, STOMP = 5701, 1901, 61701


def amqp(user, pw, vhost='/', keep=False):
    p = pika.ConnectionParameters('127.0.0.1', AMQP, vhost, pika.PlainCredentials(user, pw),
                                  socket_timeout=5, client_properties={'connection_name': 'e2e-app'})
    try:
        c = pika.BlockingConnection(p)
    except Exception:
        return None
    c.channel()
    if keep:
        return c
    c.close()
    return True


def mqtt(user, pw):
    s = socket.create_connection(('127.0.0.1', MQTT), 5)
    s8 = lambda x: struct.pack('>H', len(x.encode())) + x.encode()
    body = s8('MQTT') + bytes([4, 0xC2]) + struct.pack('>H', 30) + s8('e2e-mqtt') + s8(user) + s8(pw)
    s.send(bytes([0x10, len(body)]) + body)
    ack = s.recv(4)
    ok = len(ack) == 4 and ack[3] == 0
    if ok:
        time.sleep(0.2)
        s.send(bytes([0xE0, 0]))
    s.close()
    return ok


def stomp(user, pw):
    s = socket.create_connection(('127.0.0.1', STOMP), 5)
    s.send(f'CONNECT\naccept-version:1.2\nhost:/\nlogin:{user}\npasscode:{pw}\n\n\x00'.encode())
    ok = s.recv(200).startswith(b'CONNECTED')
    time.sleep(0.2)
    s.close()
    return ok


did = []
keep = []
for i in range(3):
    c = amqp('alice', PW, keep=(i == 0))
    did.append(('alice', 'amqp', 'ok' if c else 'fail'))
    if c is not True and c:
        keep.append(c)
for _ in range(2):
    did.append(('alice', 'amqp', 'ok' if amqp('alice', 'wrong') else 'fail'))
did.append(('ghost', 'amqp', 'ok' if amqp('ghost', 'x') else 'fail'))
did.append(('bob', 'amqp-vhost-denied', 'ok' if amqp('bob', PW, '/') else 'refused'))
did.append(('alice', 'mqtt', 'ok' if mqtt('alice', PW) else 'fail'))
did.append(('alice', 'mqtt', 'ok' if mqtt('alice', 'wrong') else 'fail'))
did.append(('alice', 'stomp', 'ok' if stomp('alice', PW) else 'fail'))
did.append(('alice', 'stomp', 'ok' if stomp('alice', 'wrong') else 'fail'))
time.sleep(1)
print(json.dumps({'did': did, 'open': len(keep)}))
sys.stdout.flush()
# keep one AMQP connection open until released
while not os.path.exists(RELEASE):
    for c in keep:
        c.process_data_events(0.2)
    time.sleep(0.2)
for c in keep:
    try:
        c.close()
    except Exception:
        pass
