# CLOUDERA-BUILD
export JAVA7_BUILD=true
export MAVEN_OPTS=${MAVEN_OPTS} -Xmx768m -XX:MaxPermSize=128m
. /opt/toolchain/toolchain.sh

mvn clean compile test-compile
