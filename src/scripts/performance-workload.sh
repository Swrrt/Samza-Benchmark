#!/usr/bin/env bash
java -cp target/benchmark-0.0.1-jar-with-dependencies.jar generator.PerformanceWorkloadGenerator AOLraw yy04:9092,yy05:9093,yy06:9094,yy07:9095,yy08:9096 5000000 /home/samza/dataset/AOL-user-ct-collection/user-ct-test-collection-01.txt
#java -cp target/benchmark-0.0.1-jar-with-dependencies.jar generator.PerformanceWorkloadGenerator AOLraw yy04:9092,yy05:9093,yy06:9094,yy07:9095,yy08:9096 10000000 /home/samza/dataset/AOL-user-ct-collection/user-ct-test-collection-01.txt
#java -cp target/benchmark-0.0.1-jar-with-dependencies.jar generator.PerformanceWorkloadGenerator AOLraw yy04:9092,yy05:9093,yy06:9094,yy07:9095,yy08:9096 20000000 /home/samza/dataset/AOL-user-ct-collection/user-ct-test-collection-01.txt
#java -cp target/benchmark-0.0.1-jar-with-dependencies.jar generator.PerformanceWorkloadGenerator AOLraw yy04:9092,yy05:9093,yy06:9094,yy07:9095,yy08:9096 50000000 /home/samza/dataset/AOL-user-ct-collection/user-ct-test-collection-01.txt

