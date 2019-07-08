package david.sc_dbscan.process

import org.apache.spark.rdd.RDD
import david.sc_dbscan.objects.Noeud
import scala.collection.mutable.HashMap
import david.sc_dbscan.objects._

object Clustring {

  def generateClusters(partitions: RDD[(Set[Int], Array[Noeud])]): RDD[(Noeud, ClusterId)] = {

    case class NoeudToCluster(noeud: Noeud, cluster: Cluster)

    //  Cluster the nodes separately in each partition
    val partialClusters: RDD[(Noeud, ClusterId)] = partitions.flatMap { partition =>

      var index: Int = 1

      var clustersList: Array[Cluster] = Array()

      val enteredNoeuds = partition._2

      val noeudsList: HashMap[Int, Noeud] = HashMap()

      enteredNoeuds.foreach { noeud =>
        //  Set node as not viisted
        noeud.setNotVisited()

        //  Each node save the id of its neighbors
        //  The hasMap allows us to get the node by its ID
        noeudsList.put(noeud.getId(), noeud)
      }

      noeudsList.foreach { noeud =>

        if (noeud._2.isCore && !noeud._2.isVisited) {

          //  The Id of the cluster is the id of partition + index
          var idC: String = partition._1.mkString(".") + "-C" + index.toString()
          var cluster = new Cluster(Set(idC))
          var condidateList: Array[Int] = Array()

          index = index + 1

          //  List of current node's neighbors
          condidateList = condidateList :+ noeud._2.getId()

          var i: Int = 0

          while (condidateList.length > i) {

            val currentNode = noeudsList.get(condidateList(i))

            //  if the node belong to the current partition
            if (currentNode.isDefined) {

              val currentNodeGet = currentNode.get

              //  if not visited yet, add to the cluster
              if (!currentNodeGet.isVisited()) {
                currentNodeGet.setVisited()

                //  Insert in the hashMap the uptdated node (setAsVisted)
                noeudsList.put(condidateList(i), currentNodeGet)

                cluster.addNoeud(currentNodeGet)

                //  if the current node is a core, add its neighbors
                if (currentNodeGet.isCore) {
                  condidateList = condidateList ++ currentNodeGet.getNeighbors().keySet
                }
              }
            }
            i = i + 1
          }
          clustersList = clustersList :+ cluster
        }
      }

      //  add the result to a list so we can loop throw it for return
      var returnNoeuds: Array[NoeudToCluster] = Array()

      clustersList.foreach { currentCluster =>

        //        var chaineSortie = currentCluster.getId().toString() + " : "
        //        println(currentCluster.getId().toString() + " " + currentCluster.getNoeuds().toSet.toString())

        currentCluster.getNoeuds().foreach { currentNoeud =>

          var newNoeud = new Noeud(currentNoeud.getId(), null, currentNoeud.getCoefficient())

          //          chaineSortie = chaineSortie + currentNoeud.getId().toString + " , "

          if (currentNoeud.isCore) {
            newNoeud.setCore()
            val r = new NoeudToCluster(newNoeud, currentCluster)
            returnNoeuds = returnNoeuds :+ r
          } else {
            returnNoeuds.dropWhile(p => p.noeud == newNoeud)
            val r = new NoeudToCluster(newNoeud, currentCluster)
            returnNoeuds = returnNoeuds :+ r
          }
        }
      }

      for (x <- 0 to returnNoeuds.size - 1) yield {
        (returnNoeuds(x).noeud, new ClusterId(returnNoeuds(x).cluster.getId()))
      }
    }

    return partialClusters
  }
}