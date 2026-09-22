// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright (c) 2026 martinx
// SPDX-License-Identifier: MPL-2.0
//
// The "Access" tab of the RabbitMQ management UI.
//
// Pages are rendered through the management UI's own render(), so navigation,
// auto-refresh and error handling behave like the built-in pages. Templates:
// RabbitMQ 4.x looks them up in COMPILED_TEMPLATES; 3.x loads
// js/tmpl/<name>.ejs, which here is one line calling the same function.

var ACCESS_UI = (function () {
    var STATE_LABEL = {
        active_24h: 'Active today', active_7d: 'Active this week', dormant: 'Dormant',
        never_used: 'Never used', attempts_only: 'Failed attempts only'
    };
    var STATE_HELP = {
        active_24h: 'logged in during the last 24 hours, or connected now',
        active_7d: 'last login between 1 and 7 days ago',
        dormant: 'last login more than 7 days ago',
        never_used: 'defined, but no successful login in the retained history',
        attempts_only: 'not defined here; only failed logins were seen'
    };

    function esc(s) {
        if (s === null || s === undefined) return '';
        return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;')
            .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    }
    function pad(n) { return (n < 10 ? '0' : '') + n; }
    function ts(ms) {
        if (!ms) return '<span class="ai-dim">&ndash;</span>';
        var d = new Date(ms);
        return d.getFullYear() + '-' + pad(d.getMonth() + 1) + '-' + pad(d.getDate()) + ' ' +
            pad(d.getHours()) + ':' + pad(d.getMinutes());
    }
    function ago(ms) {
        if (!ms) return '<span class="ai-dim">never</span>';
        var s = Math.max(0, (Date.now() - ms) / 1000);
        var t = s < 90 ? Math.round(s) + ' s' : s < 5400 ? Math.round(s / 60) + ' min' :
            s < 172800 ? Math.round(s / 3600) + ' h' : Math.round(s / 86400) + ' d';
        return '<span title="' + esc(ts(ms)) + '">' + t + ' ago</span>';
    }
    function dur(ms) {
        var s = Math.round((ms || 0) / 1000);
        if (s < 60) return s + ' s';
        if (s < 3600) return Math.round(s / 60) + ' min';
        if (s < 86400) return (s / 3600).toFixed(1) + ' h';
        return (s / 86400).toFixed(1) + ' d';
    }
    function num(n) { return (n || 0).toLocaleString(); }
    function user_link(name) {
        return '<a href="#/access/users/' + encodeURIComponent(name) + '">' + esc(name) + '</a>';
    }
    function state_pill(st) {
        return '<span class="ai-pill ai-' + esc(st) + '" title="' + esc(STATE_HELP[st] || '') + '">' +
            esc(STATE_LABEL[st] || st) + '</span>';
    }
    function counts(map, total) {
        var keys = Object.keys(map || {}).sort(function (a, b) { return map[b] - map[a]; });
        if (!keys.length) return '<span class="ai-dim">&ndash;</span>';
        return keys.map(function (k) { return esc(k) + ' <b>' + num(map[k]) + '</b>'; }).join(', ');
    }
    function bars(items, key, max, cls) {
        return items.map(function (it) {
            var v = it[key] || 0, w = max ? Math.round(100 * v / max) : 0;
            return '<div class="ai-bar ' + cls + '" style="height:' + w + '%" title="' + esc(it.day) + ': ' + v + '"></div>';
        }).join('');
    }
    function box(label, value, note) {
        return '<div class="ai-box"><div class="ai-v">' + value + '</div><div class="ai-l">' + label +
            (note ? '<div class="ai-n">' + note + '</div>' : '') + '</div></div>';
    }
    function incomplete(nodes) {
        if (!nodes || !nodes.length) return '';
        return '<p class="warning">No answer from ' + nodes.map(esc).join(', ') +
            ': figures from those nodes may be missing or up to one replication interval old.</p>';
    }
    function tabs(active) {
        var t = [['#/access', 'Overview'], ['#/access/users', 'Accounts'],
                 ['#/access/sessions', 'Sessions'], ['#/access/auth', 'Authentication']];
        return '<div class="ai-tabs">' + t.map(function (x) {
            return '<a href="' + x[0] + '"' + (x[1] == active ? ' class="ai-on"' : '') + '>' + x[1] + '</a>';
        }).join('') + '<span class="ai-ex">Report: <a href="api/access/v1/report.csv" download="accounts.csv">CSV</a> ' +
            '<a href="api/access/v1/report.html" target="_blank">HTML</a></span></div>';
    }

    // Capability guidance: say what is missing, what it affects and how to enable it.
    function notices(status) {
        if (!status || !status.capabilities) return '';
        var out = [];
        (status.capabilities.nodes || []).forEach(function (n) {
            if (!n.plugin) {
                out.push('<b>' + esc(n.node) + '</b> does not run this plugin: logins on it are not counted. ' +
                    'Run <code>rabbitmq-plugins enable rabbitmq_access_insight</code> on that node.');
                return;
            }
            if (!n.prometheus && n.metrics_export !== 'off')
                out.push('<b>' + esc(n.node) + '</b>: <code>rabbitmq_prometheus</code> is not enabled, so access metrics are served on ' +
                    'the plugin endpoint <code>' + esc(n.http.ip) + ':' + esc(n.http.port) + '/metrics</code>. ' +
                    'Enable it with <code>rabbitmq-plugins enable rabbitmq_prometheus</code> to get them on <code>:15692/metrics</code>.');
            if (!n.access_events_seen && !n.only_internal_backend)
                out.push('<b>' + esc(n.node) + '</b>: no authentication backend has reported which credential was used, ' +
                    'so methods are <i>inferred</i> from the SASL mechanism, the backend chain and the user database.');
        });
        if (!out.length) return '';
        return '<div class="ai-note">' + out.map(function (s) { return '<p>' + s + '</p>'; }).join('') + '</div>';
    }

    function overview(o, status) {
        var u = o.users, st = u.by_state || {};
        var h = '<h1>Access</h1>' + tabs('Overview') + incomplete(o.incomplete) + notices(status);
        h += '<div class="ai-boxes">' +
            box('Accounts', num(u.total), num(u.defined) + ' defined') +
            box('Never used', num(u.never_used), 'defined, no login') +
            box('In use, not defined', num(u.in_use_undefined), 'e.g. LDAP / OAuth users') +
            box('Open sessions', num(o.sessions.open)) +
            box('Logins today', num(o.logins_today.succeeded),
                num(o.logins_today.failed) + ' failed, ' + num(o.logins_today.refused) + ' refused') +
            '</div>';
        h += '<div class="section"><h2>Accounts by activity</h2><div class="hider"><table class="list"><tr><th>State</th><th>Accounts</th><th>Meaning</th></tr>';
        ['active_24h', 'active_7d', 'dormant', 'never_used', 'attempts_only'].forEach(function (k, i) {
            h += '<tr' + (i % 2 ? ' class="alt1"' : ' class="alt2"') + '><td><a href="#/access/users?state=' + k + '">' +
                state_pill(k) + '</a></td><td class="r">' + num(st[k]) + '</td><td>' + esc(STATE_HELP[k]) + '</td></tr>';
        });
        h += '</table></div></div>';
        h += '<div class="section"><h2>Authentication methods</h2><div class="hider"><table class="facts">' +
            '<tr><th>By method</th><td>' + counts(o.methods) + '</td></tr>' +
            '<tr><th>How it is known</th><td>' + counts(o.method_sources) + '</td></tr></table>' +
            '<p class="ai-dim">confirmed: reported by the authentication backend for that connection; inferred: derived from the SASL mechanism, ' +
            'the configured backends and the internal user database.</p></div></div>';
        h += '<div class="section"><h2>History on each node</h2><div class="hider"><table class="list"><tr><th>Node</th><th>Open sessions</th>' +
            '<th>Journal</th><th>Sequence</th><th>Since</th></tr>';
        (o.nodes || []).forEach(function (n, i) {
            var s = n.status || {}, j = n.journal || {};
            h += '<tr' + (i % 2 ? ' class="alt1"' : ' class="alt2"') + '><td>' + esc(n.node) + '</td><td class="r">' + num(s.open_sessions) +
                '</td><td class="r">' + (j.bytes ? (j.bytes / 1048576).toFixed(1) + ' MB' : '0') +
                '</td><td class="r">' + num(s.seq) + '</td><td>' + ts(s.started_at) + '</td></tr>';
        });
        h += '</table></div></div>';
        return h;
    }

    function users(r, params) {
        var state = params.state || '', q = params.search || '';
        var h = '<h1>Access &rsaquo; Accounts</h1>' + tabs('Accounts') + incomplete(r.incomplete);
        h += '<form class="ai-filter" onsubmit="return ACCESS_UI.filter(this)">State <select name="state"><option value="">All</option>';
        ['active_24h', 'active_7d', 'dormant', 'never_used', 'attempts_only'].forEach(function (k) {
            h += '<option value="' + k + '"' + (k == state ? ' selected' : '') + '>' + STATE_LABEL[k] + '</option>';
        });
        h += '</select> Name <input type="text" name="search" value="' + esc(q) + '"> ' +
            '<input type="submit" value="Filter" style="display:inline-block;margin:0 8px;vertical-align:middle"> ' +
            '<span class="ai-dim">' + num(r.total) + ' accounts</span></form>';
        h += '<table class="list"><tr><th>Account</th><th>Defined</th><th>State</th><th>Connected</th><th>Sessions</th>' +
            '<th>Online</th><th>Last login</th><th>Failed</th><th>Methods</th><th>Sources</th></tr>';
        r.items.forEach(function (x, i) {
            h += '<tr' + (i % 2 ? ' class="alt1"' : ' class="alt2"') + '><td>' + user_link(x.name) +
                (x.tags && x.tags.length ? ' <span class="ai-dim">' + esc(x.tags.join(' ')) + '</span>' : '') +
                '</td><td>' + (x.defined ? 'yes' + (x.has_password ? '' : ' <span class="ai-dim">(no password)</span>') : '<b>no</b>') +
                '</td><td>' + state_pill(x.state) + '</td><td class="r">' + num(x.connected) + '</td><td class="r">' + num(x.sessions) +
                '</td><td class="r">' + dur(x.online_ms) + '</td><td>' + ago(x.last_seen) + '</td><td class="r">' +
                num(x.failed + x.refused) + '</td><td>' + counts(x.methods) + '</td><td class="r">' + num(x.source_count) + '</td></tr>';
        });
        h += '</table>';
        if (r.total > r.page * r.page_size || r.page > 1) {
            h += '<p>';
            if (r.page > 1) h += '<a href="' + page_link(params, r.page - 1) + '">&larr; previous</a> ';
            h += 'page ' + r.page + ' ';
            if (r.total > r.page * r.page_size) h += '<a href="' + page_link(params, r.page + 1) + '">next &rarr;</a>';
            h += '</p>';
        }
        return h;
    }
    function page_link(params, p) {
        return '#/access/users?state=' + encodeURIComponent(params.state || '') +
            '&search=' + encodeURIComponent(params.search || '') + '&page=' + p;
    }

    function user(r) {
        var u = r.user;
        var h = '<h1>Access &rsaquo; <b>' + esc(u.name) + '</b></h1>' + tabs('Accounts') + incomplete(r.incomplete);
        h += '<div class="ai-boxes">' +
            box('State', state_pill(u.state)) +
            box('Sessions', num(u.sessions), num(u.connected) + ' connected now') +
            box('Time online', dur(u.online_ms)) +
            box('Failed logins', num(u.failed), num(u.refused) + ' refused after authentication') +
            box('Last login', ago(u.last_seen), 'first ' + ts(u.first_seen)) + '</div>';
        h += '<div class="section"><h2>Account</h2><div class="hider"><table class="facts">' +
            '<tr><th>Defined in this cluster</th><td>' + (u.defined ? 'yes' : 'no') + '</td></tr>' +
            '<tr><th>Tags</th><td>' + esc((u.tags || []).join(' ')) + '</td></tr>' +
            '<tr><th>Has a password</th><td>' + (u.has_password ? 'yes' : 'no') + '</td></tr>' +
            '<tr><th>Methods</th><td>' + counts(u.methods) + ' <span class="ai-dim">(' + counts(u.method_sources) + ')</span></td></tr>' +
            '<tr><th>Protocols</th><td>' + counts(u.protocols) + '</td></tr>' +
            '<tr><th>Virtual hosts</th><td>' + counts(u.vhosts) + '</td></tr></table></div></div>';
        var max = 0;
        r.daily.forEach(function (d) { max = Math.max(max, d.sessions, d.failed + d.refused); });
        h += '<div class="section"><h2>Last ' + r.daily.length + ' days</h2><div class="hider">' +
            '<div class="ai-chart"><div class="ai-row">' + bars(r.daily, 'sessions', max, 'ai-ok') + '</div>' +
            '<div class="ai-row ai-down">' + bars(r.daily.map(function (d) { return {day: d.day, x: d.failed + d.refused}; }), 'x', max, 'ai-bad') +
            '</div></div><p class="ai-dim">upper: sessions per day, lower: failed and refused logins. From ' +
            esc(r.daily.length ? r.daily[0].day : '') + ' (UTC days).</p></div></div>';
        h += two_col('Sources', u.sources, 'Clients', u.clients);
        h += sessions_table('Connected now', r.open_sessions, true);
        h += failures_table(r.failures, false);
        h += recent_table(r.recent);
        return h;
    }
    function two_col(t1, a, t2, b) {
        function tbl(t, list) {
            var h = '<table class="list"><tr><th>' + t + '</th><th>Sessions</th></tr>';
            (list || []).forEach(function (x, i) {
                h += '<tr' + (i % 2 ? ' class="alt1"' : ' class="alt2"') + '><td>' + esc(x.name) + '</td><td class="r">' + num(x.count) + '</td></tr>';
            });
            return h + '</table>';
        }
        return '<div class="section"><h2>' + t1 + ' and ' + t2.toLowerCase() + '</h2><div class="hider"><div class="ai-cols"><div>' +
            tbl(t1, a) + '</div><div>' + tbl(t2, b) + '</div></div></div></div>';
    }
    function sessions_table(title, list, open) {
        var h = '<div class="section"><h2>' + title + ' <span class="ai-dim">' + num(list.length) + '</span></h2><div class="hider">';
        if (!list.length) return h + '<p class="ai-dim">none</p></div></div>';
        h += '<table class="list"><tr><th>Account</th><th>From</th><th>Client</th><th>Protocol</th><th>Virtual host</th>' +
            '<th>Method</th><th>Node</th><th>' + (open ? 'Since' : 'Closed') + '</th><th>Duration</th></tr>';
        list.forEach(function (s, i) {
            var d = open ? Date.now() - s.opened_at : s.duration_ms;
            h += '<tr' + (i % 2 ? ' class="alt1"' : ' class="alt2"') + '><td>' + user_link(s.user) + '</td><td>' + esc(s.peer) +
                '</td><td>' + esc(s.client) + '</td><td>' + esc(s.protocol) + '</td><td>' + esc(s.vhost) + '</td><td>' + esc(s.method) +
                ' <span class="ai-dim">' + esc(s.method_source) + '</span></td><td>' + esc(s.node) + '</td><td>' +
                ts(open ? s.opened_at : s.at) + '</td><td class="r">' + dur(d) + (s.estimated ? '*' : '') + '</td></tr>';
        });
        return h + '</table></div></div>';
    }
    function failures_table(list, withUser) {
        var h = '<div class="section"><h2>Failed logins by source and reason</h2><div class="hider">';
        if (!list.length) return h + '<p class="ai-dim">none</p></div></div>';
        h += '<table class="list"><tr>' + (withUser ? '<th>Account</th>' : '') + '<th>From</th><th>Stage</th><th>Reason</th><th>Count</th><th>First</th><th>Last</th></tr>';
        list.forEach(function (f, i) {
            h += '<tr' + (i % 2 ? ' class="alt1"' : ' class="alt2"') + '>' + (withUser ? '<td>' + user_link(f.user) + '</td>' : '') +
                '<td>' + esc(f.source) + '</td><td>' + (f.stage == 'access' ? 'after authentication' : 'credentials') +
                '</td><td>' + esc(f.reason) + '</td><td class="r">' + num(f.count) + '</td><td>' + ts(f.first) + '</td><td>' + ts(f.last) + '</td></tr>';
        });
        return h + '</table></div></div>';
    }
    function recent_table(list) {
        var h = '<div class="section"><h2>Recent activity</h2><div class="hider">';
        if (!list.length) return h + '<p class="ai-dim">none</p></div></div>';
        h += '<table class="list"><tr><th>When</th><th>Event</th><th>Account</th><th>From</th><th>Detail</th><th>Node</th></tr>';
        list.slice(0, 200).forEach(function (r, i) {
            var ev = r.type == 'session_close' ? 'session closed' : (r.stage == 'access' ? 'refused' : 'login failed');
            var detail = r.type == 'session_close' ? esc(r.protocol) + ', ' + dur(r.duration_ms) + (r.estimated ? ' (end estimated)' : '') : esc(r.reason);
            h += '<tr' + (i % 2 ? ' class="alt1"' : ' class="alt2"') + '><td>' + ts(r.at) + '</td><td>' + ev + '</td><td>' + user_link(r.user) +
                '</td><td>' + esc(r.peer) + '</td><td>' + detail + '</td><td>' + esc(r.node) + '</td></tr>';
        });
        return h + '</table></div></div>';
    }

    function sessions(r) {
        var h = '<h1>Access &rsaquo; Sessions</h1>' + tabs('Sessions') + incomplete(r.incomplete);
        h += sessions_table('Open', r.open, true);
        h += sessions_table('Recently closed', r.recent, false);
        h += '<p class="ai-dim">* the connection ended while the plugin was not running; its end is estimated.</p>';
        return h;
    }

    function auth(r) {
        var h = '<h1>Access &rsaquo; Authentication</h1>' + tabs('Authentication') + incomplete(r.incomplete);
        var max = 0, tot = {s: 0, f: 0, r: 0};
        r.series.forEach(function (d) {
            max = Math.max(max, d.succeeded, d.failed + d.refused);
            tot.s += d.succeeded; tot.f += d.failed; tot.r += d.refused;
        });
        h += '<div class="ai-boxes">' + box('Succeeded', num(tot.s), 'last ' + r.series.length + ' days') +
            box('Failed', num(tot.f), 'wrong or unknown credentials') +
            box('Refused', num(tot.r), 'credentials accepted, then authorization or vhost refused') + '</div>';
        h += '<div class="section"><h2>Logins per day</h2><div class="hider"><div class="ai-chart"><div class="ai-row">' +
            bars(r.series, 'succeeded', max, 'ai-ok') + '</div><div class="ai-row ai-down">' +
            bars(r.series.map(function (d) { return {day: d.day, x: d.failed + d.refused}; }), 'x', max, 'ai-bad') +
            '</div></div><p class="ai-dim">upper: successful logins, lower: failed and refused (UTC days)</p></div></div>';
        h += '<div class="section"><h2>Methods</h2><div class="hider"><table class="facts"><tr><th>By method</th><td>' + counts(r.methods) +
            '</td></tr><tr><th>How it is known</th><td>' + counts(r.method_sources) + '</td></tr></table></div></div>';
        h += failures_table(r.failures, true);
        h += recent_table(r.recent_failures);
        return h;
    }

    function filter(form) {
        go_to('#/access/users?state=' + encodeURIComponent(form.state.value) +
              '&search=' + encodeURIComponent(form.search.value));
        return false;
    }

    return {overview: overview, users: users, user: user, sessions: sessions, auth: auth, filter: filter,
            esc: esc, params: {}};
})();

(function () {
    var CSS = '.ai-tabs{margin:0 0 14px;border-bottom:1px solid #ccc;padding-bottom:6px}' +
        '.ai-tabs a{margin-right:16px;text-decoration:none}.ai-tabs a.ai-on{font-weight:bold;border-bottom:2px solid #f60;padding-bottom:6px}' +
        '.ai-ex{float:right;font-size:12px}.ai-ex a{margin-left:6px}' +
        '.ai-boxes{display:flex;flex-wrap:wrap;gap:10px;margin:10px 0 18px}' +
        '.ai-box{border:1px solid #ddd;border-radius:6px;padding:10px 14px;min-width:130px;background:rgba(127,127,127,.04)}' +
        '.ai-v{font-size:22px;font-weight:bold}.ai-l{font-size:12px;color:#666}.ai-n{color:#999}' +
        '.ai-pill{display:inline-block;padding:0 7px;border-radius:9px;font-size:11px;background:#eee;color:#333;white-space:nowrap}' +
        '.ai-active_24h,.ai-active_7d{background:#d7f5dc;color:#11622a}.ai-dormant{background:#eceff2;color:#555}' +
        '.ai-never_used{background:#fff0d9;color:#8a4b00}.ai-attempts_only{background:#fde2e1;color:#8c1d18}' +
        '.ai-dim{color:#888}.ai-note{border-left:3px solid #f0a000;background:rgba(240,160,0,.08);padding:4px 12px;margin:10px 0}' +
        '.ai-filter{margin:6px 0 10px;display:flex;align-items:center;gap:6px;flex-wrap:wrap}.ai-filter input[type=submit]{margin:0 6px}.ai-cols{display:flex;gap:20px;flex-wrap:wrap}.ai-cols>div{flex:1;min-width:280px}' +
        '.ai-chart{height:120px;display:flex;flex-direction:column;border-bottom:1px solid #ccc}' +
        '.ai-row{flex:1;display:flex;align-items:flex-end;gap:2px}.ai-row.ai-down{align-items:flex-start;border-top:1px solid #ccc}' +
        '.ai-bar{flex:1;min-width:3px}' +
        '.ai-ok{background:#5aa36e}.ai-bad{background:#d9534f}td.r{text-align:right;font-variant-numeric:tabular-nums}';
    var st = document.createElement('style');
    st.appendChild(document.createTextNode(CSS));
    document.head.appendChild(st);

    var T = {
        'access-overview': function (j) { return ACCESS_UI.overview(j.access, j.status); },
        'access-users': function (j) { return ACCESS_UI.users(j.access, ACCESS_UI.params); },
        'access-user': function (j) { return ACCESS_UI.user(j.access); },
        'access-sessions': function (j) { return ACCESS_UI.sessions(j.access); },
        'access-auth': function (j) { return ACCESS_UI.auth(j.access); }
    };
    if (typeof COMPILED_TEMPLATES !== 'undefined') {
        for (var k in T) COMPILED_TEMPLATES[k] = T[k];
    }
    ACCESS_UI.templates = T;
})();

dispatcher_add(function (sammy) {
    function q(p) {
        var s = [];
        for (var k in p) if (p[k] !== '' && p[k] !== undefined) s.push(encodeURIComponent(k) + '=' + encodeURIComponent(p[k]));
        return s.length ? '?' + s.join('&') : '';
    }
    sammy.get('#/access', function () {
        render({'access': '/access/v1/overview', 'status': '/access/v1/status'}, 'access-overview', '#/access');
    });
    sammy.get('#/access/users', function () {
        var p = {state: this.params['state'] || '', search: this.params['search'] || '',
                 page: this.params['page'] || '1', sort: 'last_seen', order: 'desc'};
        ACCESS_UI.params = p;
        render({'access': '/access/v1/users' + q(p)}, 'access-users', '#/access');
    });
    sammy.get('#/access/users/:name', function () {
        render({'access': '/access/v1/users/' + esc(this.params['name'])}, 'access-user', '#/access');
    });
    sammy.get('#/access/sessions', function () {
        render({'access': '/access/v1/sessions'}, 'access-sessions', '#/access');
    });
    sammy.get('#/access/auth', function () {
        render({'access': '/access/v1/auth?days=30'}, 'access-auth', '#/access');
    });
});

NAVIGATION['Access'] = ['#/access', 'monitoring'];
