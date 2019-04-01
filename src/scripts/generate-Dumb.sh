#!/usr/bin/env bash
# Topicname, brokers, total number, speed, file1, file2...
# Speed limit is about 500000
java -cp target/benchmark-0.0.1-jar-with-dependencies.jar generator.AOLgenerator StreamBenchInput yy07:9095,yy08:9096 false 1 5000