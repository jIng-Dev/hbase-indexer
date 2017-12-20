#!/usr/bin/env bash
# CLOUDERA-BUILD
export MAVEN_OPTS=${MAVEN_OPTS} -Xmx768m -XX:MaxPermSize=128m
mvn clean compile test-compile
