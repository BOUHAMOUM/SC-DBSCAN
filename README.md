### SC-DBSCAN

We propose SC-DBSCAN, a scalable density-based clustering algorithm that operates on entities in large RDF datasets, in order to  construct a schema describing the dataset by discovering the classes of the entities. 
The main goal of SC-DBSCAN is the scalability together with efficiency, where it is designer to cluster very large RDF dataset and provide a clustering result of a quality as good as the original DBSCAN.

### How to Build
mvn install

### How to Run
The main class is: "david/sc_dbscan/Main.scala".



spark-submit --class david.sc_dbscan.Main target/sc_dbscan-0.0.1-SNAPSHOT-jar-with-dependencies.jar --eps X.X --coef X:Boolean --cap XXXX  --mpts X DataSets:String

WHERE:
--eps 	:	epsilone value
--coef 	: 	Boolean that define whether it clusters patterns or entities
--cap 	:	the capacity of a single node
--mpts 	:	minPts value
DataSet :	path to the dataset

Example:
spark-submit --class david.sc_dbscan.Main target/sc_dbscan-0.0.1-SNAPSHOT-jar-with-dependencies.jar --eps 0.8 --coef false --cap 2000  --mpts 3 /DataSets/T2800L10N10000