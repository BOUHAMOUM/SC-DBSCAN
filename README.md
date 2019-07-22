# SC-DBSCAN: a schema extraction algorithm for large RDF datasets
SC-DBSCAN is a scalable density-based clustering algorithm that operates on entities in large RDF datasets.
SC-DBSCAN builds a schema describing the entities of a dataset by discovering their classes.
SC-DBSCAN is designed to address the scalability issue of density based clustering algorithms.
It can cluster large RDF datasets and provides a clustering result of a quality as good as the original [DBSCAN](https://en.wikipedia.org/wiki/DBSCAN) algorithm.

SC-DBSCAN is implemented in [Scala](https://www.scala-lang.org/) and using the [Apache Spark](https://spark.apache.org/) framework.

## Building the project
[Maven](https://maven.apache.org/) is used to build the project.
The [Maven wrapper](https://github.com/takari/maven-wrapper) tool allows to build the project without a local maven install.
Due to some constraints imposed by the Scala compiler, a [JDK 8](https://adoptopenjdk.net/) is needed.

On Linux
```
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
./mvnw package -DskipTests
```

On Windows
```
set JAVA_HOME=C:\path\to\jdk8
mvnw.cmd package -DskipTests
```

## Running the algorithm
The main class is `david/sc_dbscan/Main.scala`.

```
spark-submit --class david.sc_dbscan.Main \\
             target/sc_dbscan-1.0-jar-with-dependencies.jar \\
             --eps X.X --coef Y --cap C  --mpts Y dataset

WHERE:
  --eps 	: the similarity threshold epsilon (between 0 and 1)
  --coef 	: a boolean that defines whether it clusters patterns or entities
  --cap 	: the maximum capacity of a computing node (in number of entities)
  --mpts 	: the density thresholg minPts
  dataset   : the path to the dataset
```

For Example
```
spark-submit --class david.sc_dbscan.Main \\
             target/sc_dbscan-1.0-jar-with-dependencies.jar \\
             --eps 0.8 --coef false --cap 2000  --mpts 3 DataSets/T2800L10N10000
```
