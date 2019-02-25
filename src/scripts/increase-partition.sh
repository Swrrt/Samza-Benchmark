#!/usr/bin/env bash
# ./increase-partition.sh TOPIC_NAME PARTITION_NUMBER
./kafka-topics.sh --zookeeper yy04:2181 --alter --topic $1 --partitions $2

