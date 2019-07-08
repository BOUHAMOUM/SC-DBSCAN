package david.sc_dbscan

import org.apache.spark.{SparkConf, SparkContext}

object PatternExtractor {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("RDF_Compression")


    val sc = new SparkContext(conf)

    val dataFile = "mushroom.txt"

    val data = sc.textFile(dataFile)

    val entites = data.map (
      ligne =>
        (ligne.split(" ").toSet, 1)
    ).reduceByKey(_+_)

//    entites.foreach(f => println(f))

    entites.saveAsTextFile("output")
  }
}
