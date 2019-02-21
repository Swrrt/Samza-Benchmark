#!/usr/bin/env bash
#mvn exec:java -Dexec.mainClass="generator.AOLgenerator" -Dexec.args="/home/samza/dataset/AOL-user-ct-collection/user-ct-test-collection-01.txt"
java -cp target/benchmark-0.0.1-jar-with-dependencies.jar generator.AOLgenerator /home/samza/dataset/AOL-user-ct-collection/user-ct-test-collection-01.txt