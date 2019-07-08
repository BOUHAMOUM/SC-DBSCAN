package david.sc_dbscan.process

import david.sc_dbscan.objects.Noeud
import org.apache.spark.rdd.RDD
import david.sc_dbscan.objects.Partition

object NoeudsReader {

//  def getNoeuds(files: RDD[String]): RDD[(Set[String], Array[Noeud])] = {
//
//    val noeudsList: RDD[(Set[String], Array[Noeud])] = files.flatMap {
//      line =>
//        val propertySet = line.split(" ").toArray
//
//        var noeud = new Noeud(line.indices.toString(), propertySet)
//
//        var partitionsList: Array[Partition] = Partitioning.affliate(noeud, noeud.getProperties())
//
//        for (x <- 0 to partitionsList.length - 1) yield {
//          (partitionsList(x).getId(), partitionsList(x).getListNoeud())
//        }
//    }
//
//    val partitions = noeudsList.reduceByKey(_ ++ _)
//
//    return partitions
//  }
}