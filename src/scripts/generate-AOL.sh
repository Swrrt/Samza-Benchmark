#!/usr/bin/env bash
#mvn exec:java -Dexec.mainClass="generator.AOLgenerator" -Dexec.args="/home/samza/dataset/AOL-user-ct-collection/user-ct-test-collection-01.txt"
#java -cp target/benchmark-0.0.1-jar-with-dependencies.jar generator.AOLgenerator /home/samza/dataset/AOL-user-ct-collection/user-ct-test-collection-01.txt
java -cp target/benchmark-0.0.1-jar-with-dependencies.jar generator.AOLgenerator StreamBenchInput yy04:9092,yy05:9093,yy06:9094,yy07:9095,yy08:9096 500000000 /home/samza/dataset/AOL-user-ct-collection/user-ct-test-collection-01.txt