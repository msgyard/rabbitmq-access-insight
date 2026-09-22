# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 martinx
# SPDX-License-Identifier: MPL-2.0
"""Broker cost of the plugin, on a node started by ../e2e/node.sh.

    python bench.py <node work dir> <user> <password> [rounds] [connections]

1. Connection churn: rounds of N connections (open, channel, close) with the
   plugin disabled and enabled in turn, on the same node. The broker's CPU
   time per connection is measured from its process, minus its idle rate.
2. Message path: publish throughput with the plugin disabled and enabled.
3. Bounds: 100k sessions and 20k distinct failed user names; the plugin's
   memory and table sizes must stay within its limits.
Needs pika and psutil.
"""
import json, multiprocessing as mp, os, statistics, subprocess, sys, time
import pika, psutil

D, USER, PW = sys.argv[1], sys.argv[2], sys.argv[3]
ROUNDS = int(sys.argv[4]) if len(sys.argv) > 4 else 3
N = int(sys.argv[5]) if len(sys.argv) > 5 else 6000
W = 6
HERE = os.path.dirname(os.path.abspath(__file__))
NODE = os.path.join(HERE, '..', 'e2e', 'node.sh')


def sh(*args):
    return subprocess.run([NODE, *args], capture_output=True, text=True).stdout.strip()


def plugin(on):
    sh('plugins', D, 'enable' if on else 'disable', 'rabbitmq_access_insight')
    time.sleep(3)


def beam():
    for p in psutil.process_iter(['name']):
        if 'beam' in (p.info['name'] or ''):
            try:
                if any(c.laddr.port == 5701 and c.status == 'LISTEN' for c in p.net_connections('tcp')):
                    return p
            except Exception:
                pass


def cpu(p):
    t = p.cpu_times()
    return t.user + t.system


def params(u=USER, pw=PW):
    return pika.ConnectionParameters('127.0.0.1', 5701, '/', pika.PlainCredentials(u, pw),
                                     heartbeat=0, socket_timeout=10)


def churn(n):
    for _ in range(n):
        c = pika.BlockingConnection(params())
        c.channel()
        c.close()


def fail_names(r):
    for i in r:
        try:
            pika.BlockingConnection(params('u%06d' % i, 'x'))
        except Exception:
            pass


def publish(n):
    c = pika.BlockingConnection(params())
    ch = c.channel()
    ch.queue_declare('bench', durable=True)
    body = b'x' * 256
    t0 = time.time()
    for _ in range(n):
        ch.basic_publish('', 'bench', body)
    c.process_data_events(0)
    ch.queue_purge('bench')
    c.close()
    return n / (time.time() - t0)


def per_conn(p):
    time.sleep(2)
    c0 = cpu(p); time.sleep(4); idle = (cpu(p) - c0) / 4
    t0 = time.time(); c1 = cpu(p)
    with mp.Pool(W) as pool:
        pool.map(churn, [N // W] * W)
    time.sleep(1.5)
    wall = time.time() - t0
    return ((cpu(p) - c1) - idle * wall) * 1000 / N, N / wall


def ev(expr):
    return sh('ctl', D, 'eval', expr)


if __name__ == '__main__':
    p = beam()
    out = {'connections_per_round': N, 'rounds': ROUNDS, 'off': [], 'on': [], 'rate_off': [], 'rate_on': []}
    for _ in range(ROUNDS):
        for on in (False, True):
            plugin(on)
            ms, rate = per_conn(p)
            out['on' if on else 'off'].append(ms)
            out['rate_on' if on else 'rate_off'].append(rate)
    off, on = statistics.mean(out['off']), statistics.mean(out['on'])
    out['cpu_ms_per_conn_off'] = round(off, 4)
    out['cpu_ms_per_conn_on'] = round(on, 4)
    out['overhead_pct'] = round(100 * (on - off) / off, 1)
    out['stdev_off'] = round(statistics.pstdev(out['off']), 4)
    out['stdev_on'] = round(statistics.pstdev(out['on']), 4)

    pub = {}
    for on in (False, True):
        plugin(on)
        pub['on' if on else 'off'] = round(statistics.mean([publish(50000) for _ in range(3)]))
    out['publish_msgs_per_s'] = pub
    out['events_during_publish'] = 'see seq_before/after'
    s0 = int(ev('rabbit_access_insight_collector:seq().').splitlines()[-1])
    publish(50000)
    s1 = int(ev('rabbit_access_insight_collector:seq().').splitlines()[-1])
    out['records_written_by_50k_publishes'] = s1 - s0 - 2   # the publisher's own open and close

    # bounds: 100k sessions in total, then 20k distinct failing user names
    plugin(True)
    ev('application:set_env(rabbitmq_access_insight, max_users, 10000).')
    done = sum(out['off']) and 0
    with mp.Pool(W) as pool:
        pool.map(churn, [100000 // W] * W)
    with mp.Pool(W) as pool:
        pool.map(fail_names, [range(k, 20000, W) for k in range(W)])
    time.sleep(3)
    mem = ev('lists:sum([ets:info(T, memory) * erlang:system_info(wordsize) || '
             'T <- [rai_user, rai_daily, rai_fail, rai_origin, rai_session, rai_recent, rai_ctr]]).')
    out['bounds'] = {
        'plugin_table_bytes': int(mem.splitlines()[-1]),
        'user_rows': int(ev('ets:info(rai_user, size).').splitlines()[-1]),
        'recent_rows': int(ev('ets:info(rai_recent, size).').splitlines()[-1]),
        'open_sessions': int(ev('ets:info(rai_session, size).').splitlines()[-1]),
        'collector_heap_bytes': int(ev('element(2, erlang:process_info(whereis(rabbit_access_insight_collector), memory)).').splitlines()[-1]),
        'journal': ev('rabbit_access_insight_journal:info().').replace('\n', ' '),
    }
    print(json.dumps(out, indent=1))
