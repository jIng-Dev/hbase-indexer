#!/usr/bin/env bash

set -ex

export MAVEN_OPTS="${MAVEN_OPTS} -Xmx768m -XX:MaxPermSize=128m"

# activate mvn-gbn wrapper
mv "$(which mvn-gbn-wrapper)" "$(dirname "$(which mvn-gbn-wrapper)")/mvn"

mvn -U clean compile test-compile
