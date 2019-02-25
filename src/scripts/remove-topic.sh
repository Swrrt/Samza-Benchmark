#!/usr/bin/env bash
# ./remove-topic.sh TOPICNAME
#
# Remember to remove topics in zookeeper: /brokers/topics/TOPICNAME
#
./bin/kafka-topics --delete --zookeeper yy04:2181 --topic $1