#!/usr/bin/env bash
# JobName
java -cp target/benchmark-0.0.1-jar-with-dependencies.jar metric.SamzaMetricMonitor yy04:9092,yy05:9093,yy06:9094,yy07:9095,yy08:9096 metrics $1