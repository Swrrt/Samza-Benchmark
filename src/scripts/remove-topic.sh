#!/usr/bin/env bash
# ./remove-topic.sh TOPICNAME
# Manually:
# Stop kafka server
# Remove topics in zookeeper: /brokers/topics/TOPICNAME
# Connect to brokers, rm -rf /tmp/kafka-logs/MyTopics-
# Start kafka


# Add config "delete.topic.enable=true" to server.properties

kafka/bin/kafka-topics --delete --zookeeper yy04:2181 --topic $1

#
# Temporarily update the retention time to one second
#
kafka/bin/kafka-topics.sh --zookeeper yy04:2181 --alter --topic $1 --config retention.ms=1000

# Restore retention
# kafka/bin/kafka-topics.sh --zookeeper yy04:2181 --alter --topic $1 --deleteConfig retention.ms
