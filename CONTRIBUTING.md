# Contributing

Issues and pull requests are welcome.

## Build and test

```sh
./run-tests.sh --rmq-release 4.3.6     # EUnit against a RabbitMQ release's rabbit_common
./build-ez.sh  --rmq-release 4.3.6     # the installable .ez for the OTP you run
```

CI runs the suite against every supported RabbitMQ line; please make sure it
passes locally against at least one.

## Sign off your commits

Contributions are accepted under the project's license, the Mozilla Public
License 2.0. By adding a `Signed-off-by` line to each commit you certify the
[Developer Certificate of Origin](https://developercertificate.org/): that you
wrote the change, or otherwise have the right to submit it under that license.

```sh
git commit -s
```
