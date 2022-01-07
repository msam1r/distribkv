#!/bin/bash
set -e

trap 'killall distribkv' SIGINT

cd $(dirname $0)

killall distribkv || true
sleep 0.1

go install -v

distribkv -db-path=tmp/minia.db -http-addr=127.0.0.1:8080 -config-file=sharding.toml -shard=Minia &
distribkv -db-path=tmp/cairo.db -http-addr=127.0.0.1:8081 -config-file=sharding.toml -shard=Cairo &
distribkv -db-path=tmp/giza.db -http-addr=127.0.0.1:8082 -config-file=sharding.toml -shard=Giza &
distribkv -db-path=tmp/aswan.db -http-addr=127.0.0.1:8083 -config-file=sharding.toml -shard=Aswan &

wait
