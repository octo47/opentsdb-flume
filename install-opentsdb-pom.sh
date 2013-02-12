mvn install:install-file  -Dfile=opentsdb/target/opentsdb-1.1.0.jar \
                          -Dsources=opentsdb/target/opentsdb-1.1.0-sources.jar \
			  -DpomFile=opentsdb/pom.xml \
                          -DlocalRepositoryPath=repo
