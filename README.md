# rabbitmq_access_insight

Access Insight for RabbitMQ — a plugin that answers **who connects to your
broker, from where, and how they authenticate**, from inside the broker.

> **Status: in development.** Nothing here is ready for production yet. The
> first usable release (0.1.0) is being built; this README describes what it
> is meant to do.

## What it will do

- **Accounts** — every defined user against actual logins: first and last
  seen, sessions, time online, sources and clients; which accounts are in
  use, which were never used, which log in without being defined.
- **Authentication** — login results by stage (credentials, authorization,
  virtual host), failures by account, source and reason, and the
  authentication method of each login, marked as confirmed or inferred.
- **Sessions** — current and past connections, kept on each node's disk and
  queryable from any node.
- **Outputs** — an *Access* tab in the management UI, `/api/access/v1/*`, and
  access metrics on the Prometheus endpoint (`:15692/metrics`) or on the
  plugin's own endpoint when `rabbitmq_prometheus` is not enabled.

It observes only: it never takes part in allowing or refusing a connection,
and never stores passwords, tokens or message bodies. It works on unmodified
official RabbitMQ releases, 3.12 to 4.3.

## Build and test

```sh
./run-tests.sh --rmq-release 4.3.6     # EUnit against a RabbitMQ release's rabbit_common
./build-ez.sh  --rmq-release 4.3.6     # the installable .ez for the OTP you run
```

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[Mozilla Public License 2.0](LICENSE), the same license as RabbitMQ.

This is an independent community project, part of
[msgyard](https://msgyard.bitey.ai). It is not affiliated with, endorsed or
sponsored by Broadcom or the RabbitMQ team. RabbitMQ is a trademark of
Broadcom Inc. and its subsidiaries.
