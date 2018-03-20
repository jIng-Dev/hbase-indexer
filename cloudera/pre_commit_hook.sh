#!/usr/bin/env bash

set -ex

export MAVEN_OPTS="${MAVEN_OPTS} -Xmx768m -XX:MaxPermSize=128m"

# activate mvn-gbn wrapper
mv "$(which mvn-gbn-wrapper)" "$(dirname "$(which mvn-gbn-wrapper)")/mvn"

mvn --update-snapshots --batch-mode -Dmaven.test.failure.ignore=true -Dtests.nightly=true --fail-at-end clean install
