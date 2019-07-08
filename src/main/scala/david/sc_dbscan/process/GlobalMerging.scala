package david.sc_dbscan.process


import org.apache.spark.rdd.RDD
import david.sc_dbscan.objects._

import scala.collection.mutable.HashMap

object GlobalMerging {

  //  Group the clusters that contain common entity
  //  Input : (entity, cluster)
  //  Output : Merger cluster
  def clustersMerging(partialClusters: RDD[(Noeud, ClusterId)], mergeLoop: Int): HashMap[Set[String], Set[Int]] = {

    case class FinalClusters(clusterId: Set[String], Nodes: Set[Int])

    //  Define the clusters having common entity
    //  Group clusters having the same entity
    var clustersToMerge = partialClusters.reduceByKey { (x, y) =>

      //  Create a new cluster which merges the entities contained in the clusters grouped together
      //  Identifies the clusters that should be merged
      var cluster = x.getId() ++ y.getId()

      new ClusterId(cluster)
    }


//    val intermediateClusters = clustersToMerge.map( x => (x._2, Array(x._1)) ).reduce{
//      (x, y) =>
//        ( new ClusterId(x._1.getId() ++ y._1.getId()),  x._2 ++ y._2)
//    }



//    clustersToMerge.foreach(f => println(f._1+" : "+f._2))

//    clustersToMerge.foreach(p => println(p._1 + " " + p._2.getId().size))

//    clustersToMerge.foreach(p => println(p._1 + " " + p._2.getId()))

//    println("Before merging : " + clustersToMerge.count())

    //    clustersToMerge.foreach(println)
    println("--------------")
    var clusters = clustersToMerge.map { noeudToCluster =>

      var id = new ClusterId(Set())

      if (noeudToCluster._1.isCore()) {
        id = noeudToCluster._2
      }
      else {
        id = new ClusterId(Set(noeudToCluster._2.getId().last))
      }

//      val clusterId = id
      //  Use ClusterId instead of Cluster to reduce the size of the output
      (id, new FinalClusters(id.getId(), Set(noeudToCluster._1.getId())))
    }.reduceByKey{
      (x, y)=>
        val patterns = x.Nodes ++ y.Nodes
        val idClusters = x.clusterId ++ y.clusterId

        new FinalClusters(idClusters, patterns)
    }



    for(i <- 1 to mergeLoop)
      {
        var clusterLoop = clusters

//        println("CLUSTERS BEFORE MERGING : "+clusterLoop.count())

        var clusters1 = clusterLoop.map{
          c =>
//            println(c._1.hashCode)
            (new ClusterId(c._2.clusterId), c._2)
        }.reduceByKey
        {
          (x, y) =>
            new FinalClusters(x.clusterId ++ y.clusterId, x.Nodes ++ y.Nodes)
        }

        clusters = clusters1
      }

    var finalClusters: HashMap[Set[String], Set[Int]] = HashMap()

    clusters.collect().foreach { currentCluster =>

      var id: Set[String] = currentCluster._2.clusterId
      var elements: Set[Int] = currentCluster._2.Nodes

      finalClusters.foreach { candidateCluster =>
        if (!id.intersect(candidateCluster._1).isEmpty) {

          id = id.++:(candidateCluster._1)
          elements = elements.++:(candidateCluster._2)

          finalClusters.remove(candidateCluster._1)
        }
      }

      finalClusters.put(id, elements)
    }

//    val logger = Logger.getLogger(getClass.getName)
//    logger.fatal("Partial Clusters : " + clusters.count())


    //finalClusters.foreach(p => println(p._1.toString()+" -> "+p._2.toString()))

    return finalClusters
  }
}