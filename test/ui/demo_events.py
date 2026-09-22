# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 martinx
# SPDX-License-Identifier: MPL-2.0
"""Write 30 days of fictional access events for the screenshots, as Erlang
terms: [{Node, Type, Props, TimestampMs}]. Addresses are from private and
documentation ranges; names are made up.

    python demo_events.py <out file> <node0> <node1> <node2>
"""
import random, sys, time

random.seed(7)
OUT, NODES = sys.argv[1], sys.argv[2:5]
NOW = int(time.time() * 1000)
DAY, H = 86400000, 3600000
pid_n = [900000]

SERVICES = [
    # name, protocol, client (product, version) or mqtt id, sources, sessions/day, method
    ('orders-service', (0, 9, 1), ('Spring AMQP', '3.1.4'), ['10.20.1.11', '10.20.1.12', '10.20.1.13'], 6, 'password'),
    ('billing-api', (0, 9, 1), ('RabbitMQ .NET client', '6.8.1'), ['10.20.2.21', '10.20.2.22'], 4, 'token'),
    ('inventory-sync', (0, 9, 1), ('amqp091-go', '1.10.0'), ['10.20.3.31'], 3, 'token'),
    ('reporting-etl', (0, 9, 1), ('pika', '1.3.2'), ['10.20.4.41'], 1, 'password'),
    ('iot-gateway', ('MQTT', (3, 1, 1)), 'gw-edge-07', ['10.20.8.70', '10.20.8.71', '10.20.8.72', '10.20.8.73'], 8, 'password'),
    ('notification-worker', ('STOMP', '1.2'), ('STOMP client', ''), ['10.20.5.51'], 2, 'password'),
    ('payment-callback', (0, 9, 1), ('Spring AMQP', '3.1.4'), ['192.0.2.14', '192.0.2.15'], 5, 'token'),
    ('ops-monitor', (0, 9, 1), ('amqp091-go', '1.10.0'), ['10.20.9.90'], 1, 'password'),
    ('svc-ldap-analytics', (0, 9, 1), ('pika', '1.3.2'), ['10.20.6.61'], 1, 'other'),
]
DORMANT = [('legacy-batch', 19), ('dev-sandbox', 11)]


def pid():
    pid_n[0] += 1
    return ('fakepid', pid_n[0])


def props_login(user, proto, src, port, mech='PLAIN'):
    name = f'{src}:{port} -> 10.20.0.5:5672'
    return name, [('connection_type', 'network'), ('name', user), ('connection_name', name),
                  ('peer_host', tuple(int(x) for x in src.split('.'))), ('peer_port', port),
                  ('protocol', proto), ('auth_mechanism', mech)]


def client_props(client):
    if isinstance(client, str):
        return [('client_properties', [('client_id', 'longstr', client)])]
    prod, ver = client
    cp = [('<<"product">>', 'longstr', prod)] + ([('<<"version">>', 'longstr', ver)] if ver else [])
    return [('client_properties', cp)]


events = []


def session(node, user, proto, client, src, t_open, t_close, method):
    port = random.randint(40000, 65000)
    name, lp = props_login(user, proto, src, port)
    p = pid()
    events.append((node, 'user_authentication_success', lp, t_open - 5))
    if method == 'token':
        events.append((node, 'access_auth_verified',
                       [('schema_version', 1), ('stage', 'verified'), ('user', user), ('method', 'token'),
                        ('backend', 'example_token_backend'), ('credential', 'prefixed'), ('alg', 'RS256'),
                        ('pid', p), ('exp', (t_open + 30 * DAY) // 1000), ('iss', 'https://idp.example')], t_open - 3))
    created = [('pid', p), ('name', name), ('user', user), ('vhost', '/' if user != 'iot-gateway' else 'iot'),
               ('protocol', proto), ('peer_host', tuple(int(x) for x in src.split('.'))), ('peer_port', port),
               ('auth_mechanism', 'PLAIN'), ('type', 'network'), ('connected_at', t_open)] + client_props(client)
    events.append((node, 'connection_created', created, t_open))
    if t_close:
        events.append((node, 'connection_closed', [('pid', p), ('name', name)], t_close))


def failure(node, user, src, t, reason):
    port = random.randint(40000, 65000)
    _, lp = props_login(user, (0, 9, 1), src, port)
    events.append((node, 'user_authentication_failure', lp + [('error', reason)], t))


def refused(node, user, src, t):
    port = random.randint(40000, 65000)
    name, lp = props_login(user, (0, 9, 1), src, port)
    events.append((node, 'user_authentication_success', lp, t))
    events.append((node, 'connection_closed', [('pid', pid()), ('name', name)], t + 20))


for d in range(29, -1, -1):
    base = NOW - d * DAY
    for i, (user, proto, client, srcs, per_day, method) in enumerate(SERVICES):
        node = NODES[i % 3]
        n = max(1, int(per_day * random.uniform(0.6, 1.3)))
        for k in range(n):
            t0 = base - random.randint(1 * H, 23 * H)
            if t0 > NOW - 60000:
                continue
            last = d == 0 and k == 0
            t1 = None if last else min(NOW - 30000, t0 + random.randint(5 * 60000, 10 * H))
            session(node, user, proto, client, random.choice(srcs), t0, t1, method)
    # background noise: occasional wrong passwords from the apps themselves
    if random.random() < 0.4:
        failure(NODES[0], 'reporting-etl', '10.20.4.41', base - random.randint(H, 20 * H),
                "user 'reporting-etl' - invalid credentials")
    if random.random() < 0.25:
        refused(NODES[1], 'payment-callback', '192.0.2.15', base - random.randint(H, 20 * H))
for user, days_ago in DORMANT:
    for k in range(4):
        t0 = NOW - days_ago * DAY - k * DAY - random.randint(H, 10 * H)
        session(NODES[2], user, (0, 9, 1), ('pika', '1.3.2'), '10.20.7.77', t0, t0 + 40 * 60000, 'password')
# a password-guessing burst two days ago
for k in range(60):
    u = random.choice(['admin', 'test', 'root', 'guest1', 'rabbit'])
    failure(NODES[0], u, '203.0.113.' + str(random.choice([17, 17, 17, 44])), NOW - 2 * DAY + k * 45000,
            f"user '{u}' - invalid credentials")


def term(v):
    if isinstance(v, tuple) and len(v) == 2 and v[0] == 'fakepid':
        return '{fakepid,%d}' % v[1]
    if isinstance(v, tuple):
        return '{' + ','.join(term(x) for x in v) + '}'
    if isinstance(v, list):
        return '[' + ','.join(term(x) for x in v) + ']'
    if isinstance(v, int):
        return str(v)
    if isinstance(v, str) and v.startswith('<<"'):
        return v
    if v in ('network', 'longstr', 'verified', 'token', 'prefixed', 'PLAIN_atom', 'MQTT', 'STOMP'):
        return v if v not in ('MQTT', 'STOMP') else "'%s'" % v
    if v == 'client_id':
        return 'client_id'
    return '<<"%s">>' % v.replace('\\', '\\\\').replace('"', '\\"')


def prop(k, v):
    key = k if not k.startswith('<<') else k
    return '{%s,%s}' % (key, term(v))


with open(OUT, 'w') as f:
    for node, typ, props, ts in sorted(events, key=lambda e: e[3]):
        ps = []
        for kv in props:
            if len(kv) == 3:   # client property triple
                ps.append('{%s,longstr,%s}' % (kv[0] if kv[0].startswith('<<') else kv[0], term(kv[2])))
            else:
                k, v = kv
                if k == 'client_properties':
                    ps.append('{client_properties,[%s]}' % ','.join(
                        '{%s,longstr,%s}' % (c[0], term(c[2])) for c in v))
                elif k in ('protocol',) and isinstance(v, tuple) and isinstance(v[0], str):
                    ps.append("{protocol,{'%s',%s}}" % (v[0], term(v[1]) if not isinstance(v[1], str) else '"%s"' % v[1]))
                else:
                    ps.append('{%s,%s}' % (k, term(v)))
        f.write("{'%s',%s,[%s],%d}.\n" % (node, typ, ','.join(ps), ts))
print(len(events))
