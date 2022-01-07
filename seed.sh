#!/bin/bash

shards=(localhost:8080 localhost:8081)

for shard in ${shards[@]}; do
    echo $shard
    for i in {1..1000}; do
        curl "http://$shard/set?key=key-$RANDOM&value=value-$RANDOM"
    done
done
