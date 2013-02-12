Module for flume, allows to write incoming events
directly to OpenTSDB. Source module allows to 
emulate OpenTSDB server and accept incoming events.

How To Build:
============

1. Build opentsdb using pom.xml
2. Use install-opentsdb-pom.sh path/where/opentsdb/built, this script installs
opentsdb into local repo directory
3. Build with mvn install
