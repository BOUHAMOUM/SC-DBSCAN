package david.sc_dbscan.process

import org.apache.spark.rdd.RDD
import david.sc_dbscan.objects.Noeud
import scala.collection.mutable.HashMap
import david.sc_dbscan.Main

object CoresIdentification {

  def coresIdentification(partitions: RDD[(Set[Int], Array[Noeud])], minPts: Int, eps: Double): RDD[(Set[Int], Array[Noeud])] = {

    //  Structure that assign to each node the partition it belongs to and its neighbors
    case class NeighborsByPartition(partitions: Array[Set[Int]], neighbors: HashMap[Int, Int])

    val partialNeighbors: RDD[(Noeud, NeighborsByPartition)] = partitions.flatMap {

      part =>
        var partitionId = part._1

        var result: Array[Noeud] = Array()

        //  Cartesian product to find out neighbors for each node
        part._2.foreach { currentNoeud =>

          //  We do not need the properties describing the pattern so set to null
          val newNoeud = new Noeud(currentNoeud.getId(), null, currentNoeud.getCoefficient())

          part._2.foreach { noeud =>

            if (noeud != currentNoeud && Compare.similar(currentNoeud, noeud, eps)) {
              newNoeud.addNeighbor(noeud.getId(), noeud.getCoefficient())
            }
          }

          result = result :+ newNoeud
        }

        for (i <- 0 to result.length - 1) yield {
          //  for each node, return the partition it belongs to and its neighbors in the current partition
          val neighborsPartition = new NeighborsByPartition(Array(partitionId), result(i).getNeighbors())
          (result(i), neighborsPartition)
        }
    }

    //  for each node, group its neighbors finded out throw the different partition
    val totalNeighbors: RDD[(Noeud, NeighborsByPartition)] = partialNeighbors.reduceByKey { (x, y) =>

      var neighborsList = x.neighbors.++:(y.neighbors)
      var partitions = x.partitions.++:(y.partitions)

      new NeighborsByPartition(partitions.distinct, neighborsList)
    }

    //  repartition the data according to the partitionID
    val newNoeuds = totalNeighbors.flatMap { noeud =>

      //  We do not need the properties describing the pattern
      val newNoeud = new Noeud(noeud._1.getId(), null, noeud._1.getCoefficient())

      //  If the number of neighbors (the entities represented by the patter - the currentEntity) + the entities represented by similar patterns
      if (noeud._2.neighbors.values.sum + newNoeud.getCoefficient() - 1 >= minPts)
      {
        newNoeud.setCore()
        newNoeud.setNeighborList(noeud._2.neighbors)
        //            Main.corpsNumber += 1

        //            println(newNoeud.getId()+" .")
      }

      val partitions = noeud._2.partitions
      val partitionsSize = partitions.length - 1

      for (i <- 0 to partitionsSize) yield {
        var resultList: HashMap[Int, Noeud] = HashMap()
        resultList.put(newNoeud.getId(), newNoeud)
        (partitions(i), Array(newNoeud))
      }
    }

    //  Return for each partition, the list of nodes
    val updatedNoeuds = newNoeuds.reduceByKey { (x, y) => x.++:(y) }

    return updatedNoeuds
  }
}