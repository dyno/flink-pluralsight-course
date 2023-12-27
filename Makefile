SHELL = /bin/bash


BASEPATH := /usr/local/Cellar/apache-flink/1.18.0/libexec

start-cluster:
	$(BASEPATH)/bin/start-cluster.sh

# https://central.sonatype.com/artifact/org.apache.flink/flink-quickstart-scala/versions
# https://mvnrepository.com/artifact/org.apache.flink/flink-quickstart-scala
generate-project:
	mvn archetype:generate                         \
		-DarchetypeGroupId=org.apache.flink          \
		-DarchetypeArtifactId=flink-quickstart-scala \
		-DarchetypeVersion=1.16.3                    \
		-DgroupId=flink-pluralsight-course           \
		-DartifactId=flink-pluralsight-course        \
		-Dversion=1.0                                \
		-Dpackage=com.pluralsight.flink              \
		# END

download-dataset:
	[ -d src/main/resources/ml-latest-small ] || \
	( cd src/main/resources && \
		curl -O https://files.grouplens.org/datasets/movielens/ml-latest-small.zip && \
	  	unzip ml-latest-small.zip \
	)