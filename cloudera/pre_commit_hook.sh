# CLOUDERA-BUILD
export JAVA7_BUILD=true
. /opt/toolchain/toolchain.sh

mvn -Dhbase.api=1.0 clean compile test-compile
