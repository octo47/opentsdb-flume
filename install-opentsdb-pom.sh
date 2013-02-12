if [ "$1x" == "x" ]; then
   echo "Usage: $0 path-to-opentsdb-builddir"
   exit 1
fi
mvn install:install-file  -Dfile=$1/target/opentsdb-1.1.0.jar \
                          -Dsources=$1/target/opentsdb-1.1.0-sources.jar \
			  -DpomFile=$1/pom.xml \
                          -DlocalRepositoryPath=repo
