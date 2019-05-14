#!/usr/bin/env bash
#mvn exec:java -Dexec.mainClass="generator.AOLFixedGenerator" -Dexec.args="/home/samza/dataset/AOL-user-ct-collection/user-ct-test-collection-01.txt"
#java -cp target/benchmark-0.0.1-jar-with-dependencies.jar generator.AOLFixedGenerator /home/samza/dataset/AOL-user-ct-collection/user-ct-test-collection-01.txt

# Topicname, brokers, total number, speed, file1, file2...
# Speed limit is about 500000
java -cp target/benchmark-0.0.1-jar-with-dependencies.jar generator.AOLPoissonGenerator StreamBenchInput yy04:9092,yy05:9093,yy06:9094,yy07:9095,yy08:9096 500000000 500000 /home/samza/dataset/AOL-user-ct-collection/user-ct-test-collection-01.txt