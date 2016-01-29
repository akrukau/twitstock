#!/usr/bin/env bash
# 
# Purges Kafka queues for topics specified in the script arguments 
# From http://stackoverflow.com/questions/16284399/purge-kafka-queue
#
for topic in "$@"
do
    kafka-topics.sh --zookeeper localhost:2181 --alter --topic $topic --config retention.ms=1
    sleep 1m
    kafka-topics.sh --zookeeper localhost:2181 --alter --topic $topic --delete-config retention.ms
done

