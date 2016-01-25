#!/usr/bin/env bash

# From http://stackoverflow.com/questions/16284399/purge-kafka-queue

for topic in "$@"
do
    kafka-topics.sh --zookeeper localhost:2181 --alter --topic $topic --config retention.ms=1000
    sleep 1m
    kafka-topics.sh --zookeeper localhost:2181 --alter --topic $topic --delete-config retention.ms
done

