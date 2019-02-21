#!/usr/bin/env bash
mvn exec:java -Dexec.mainClass="generator.AOLgenerator" -Dexec.args="/home/samza/dataset/AOL-user-ct-collection/user-ct-test-collection-01.txt"