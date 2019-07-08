package david.sc_dbscan.process

import david.sc_dbscan.objects.{NodeBuilder, Noeud, Partition, SetAccumulator}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.HashMap


object Partitioning {

  var noeudIndex = 0

  //TODO: corriger le nom, pourquoi le 1er paramètre ne suffit pas ?, Array est inefficace ici
  /**
    * Compute the partitions where a node should belong to.
    *
    * @param noeud the node
    * @param prop  the properties of the node
    * @return
    */

  def affliate(noeud: Noeud, prop: Set[Int], eps: Double, order: Map[Int, Int]): Array[Partition] = {

    var partitions: Array[Partition] = Array()


    var finalPropertiesOrdered: List[Int] = orderProperties(noeud.getProperties(), order)

    var nomberPartitions = finalPropertiesOrdered.size - Math.ceil(finalPropertiesOrdered.size * eps).toInt + 1

    if (nomberPartitions > finalPropertiesOrdered.size)
      nomberPartitions = finalPropertiesOrdered.size

    //    Generate a partition for each property
    for (i <- 0 to nomberPartitions - 1) {
      var p = new Partition(Set(finalPropertiesOrdered(i)))

      p.addNoeud(noeud)

      partitions = partitions :+ p
    }

    return partitions
  }

  //  ----------------------------------------------------------
  //  Generate the first partitions according to the propertySet
  //  ----------------------------------------------------------
  def getInitalPartition(files: RDD[String], eps: Double, coefficient: Boolean, order: Map[Int, Int]): RDD[(Set[Int], Array[Noeud])] = {

    //    val propertyList = Main.sc.accumulator(Set(""))(SetAccumulator)

    val noeudsList: RDD[(Set[Int], Array[Noeud])] = files.flatMap {
      line =>

        var partitionsList: Array[Partition] = Array()

        if (!line.equals("") && !line.equals(" +")) {
          var noeud = NodeBuilder.createNode(line, coefficient)

          //  Get the partitions according to the property set (partition by property)
          //  If the node was well created
          if (noeud.getProperties() != null) {
            partitionsList = Partitioning.affliate(noeud, noeud.getProperties(), eps, order)
          }
        }

        //        for (i <- 0 to nomberPartitions - 1) yield {
        for (i <- 0 to partitionsList.length - 1) yield {
          //                    println(partitionsList(i).getId().toString() + " -> "+partitionsList(i).getListNoeud().toList.toString())
          (partitionsList(i).getId(), partitionsList(i).getListNoeud())
        }
    }

    val partitions = noeudsList.reduceByKey(_ ++ _)

    return partitions
  }

  //  --------------------------------------------------------------
  //  Generate the Final Partitions
  //  --------------------------------------------------------------
  def getFinalPartitions(initialPartition: RDD[(Set[Int], Array[Noeud])], nodeCapacity: Int, eps: Double, order: Map[Int, Int]): RDD[(Set[Int], Array[Noeud])] = {

    var finalPartitions = initialPartition.flatMap { f =>

      var p = new Partition(f._1)
      p.addNoeuds(f._2)

      //      println(f._1.toString()+" -> "+f._2.toSet)

      //  In each partition sent to a clustering node, get partitions with size smaller that nodeCapacity
      var result: Array[Partition] = Partitioning.getSmallerPartitions(p, nodeCapacity, eps, order)

      for (i <- 0 to result.length - 1) yield {
        (result(i).getId(), result(i).getListNoeud())
      }
    }


    //    return the distinct list of partitions
    //    Delete the duplicated partitions
    //    val finalSmallPartitions = finalPartitions.reduceByKey((x, y) => x)
    //
    //    finalSmallPartitions.map( f => (f._1, f._2.toList.toString()) ).saveAsTextFile("partitions")

    //    finalPartitions.map( f => (f._1+" "+f._2.toList.toString()) ).saveAsTextFile("partitions")

    return finalPartitions
  }

  //  ----------------------------------------------------------
  //  Generate small partitions from a given partition
  //  ----------------------------------------------------------
  def getSmallerPartitions(initialPartition: Partition, nodeCapacity: Int, eps: Double, order: Map[Int, Int]): Array[Partition] = {

    var finalPartitions: Array[Partition] = Array()
    val initialPartitionSize = initialPartition.getListNoeud().length

    if (initialPartitionSize > 1) {
      if (initialPartitionSize <= nodeCapacity) {
        //      Add to output the small partitions
        finalPartitions = finalPartitions :+ initialPartition
      } else {
        // Repartition the big partitions
        val newPartitions: Array[Partition] = repartition(initialPartition, initialPartition.getId(), eps, order)

        //  Repartition recursively the big partitions
        newPartitions.foreach { f =>
          finalPartitions = finalPartitions ++ getSmallerPartitions(f, nodeCapacity, eps, order)
        }
      }
    }


    return finalPartitions
  }

  //  ------------------------------------------------------------
  //  ------------------------------------------------------------
  def repartition(partitions: Partition, superId: Set[Int], eps: Double, order: Map[Int, Int]): Array[Partition] = {
    var resultPartitions: Array[Partition] = Array()

    var list: HashMap[Set[Int], Array[Noeud]] = HashMap()

    //  Containts the nodes that have a propertySet equals to partition's ID
    //  These nodes should be in all the sub-partitions
    var nodeToDuplicate: Array[Noeud] = Array()

    partitions.getListNoeud().foreach {
      f =>

        //        if (Compare.similar(f, partitions.getId(), 0.7))
        //          println("ERROR SIMILARITY : " + f.getId())


        if (f.getProperties().equals(superId) || f.getProperties().subsetOf(superId)) {
          //  if the id of partition is equals to entity's propertySet
          //  These nodes should be in all the sub-partitions

          //          list.put(superId, Array(f))

          nodeToDuplicate = nodeToDuplicate.:+(f)

        } else {

          val p = orderProperties(f.getProperties(), order)

          var nomberPartitions = p.size - Math.ceil(p.size * eps).toInt + 1 + (superId.size)

          if (nomberPartitions > p.size)
            nomberPartitions = p.size

          for (i <- 0 to nomberPartitions - 1) {
            val pr = p(i)
            if (!superId.contains(pr)) {
              if (list.contains(Set(pr))) {
                var r: Array[Noeud] = list.apply(Set(pr))
                r = r :+ f
                list.put(Set(pr), r.distinct)
              } else {
                list.put(Set(pr), Array(f))
              }
            }
          }

          //  Repartition the data according to another property
          //          f.getProperties().foreach {
          //            p =>
          //              val pr = p
          //              if (!superId.contains(pr)) {
          //                if (list.contains(Set(pr))) {
          //                  var r: Array[Noeud] = list.apply(Set(pr))
          //                  r = r :+ f
          //                  list.put(Set(pr), r.distinct)
          //                } else {
          //                  list.put(Set(pr), Array(f))
          //                }
          //              }
          //          }
        }
    }

    if (list.size == 0) {
      println("ERROR : " + superId.toString())
      println(nodeToDuplicate.length)
    }

    if( nodeToDuplicate.length != partitions.getListNoeud().length )
    {
      list.foreach {
        f =>
          var p = new Partition(superId ++ f._1)
          p.addNoeuds(f._2)

          p.addNoeuds(nodeToDuplicate)

          resultPartitions = resultPartitions :+ p
      }
    }


    return resultPartitions
  }

  def orderProperties(propertySet: Set[Int], order: Map[Int, Int]): List[Int] = {

    var orderedHashMap: Map[Int, Array[Int]] = Map()
    var finalPropertSet: List[Int] = List()

    propertySet.foreach {
      f =>

        val x = order.get(f).get

        if (orderedHashMap.get(x) == None) {
          orderedHashMap = orderedHashMap.+(x -> Array(f))
        }
        else {
          var list = orderedHashMap.get(x).get

          list = list.:+(f)

          orderedHashMap = orderedHashMap.+(x -> list)
        }
    }

    val keysList = orderedHashMap.keys.toArray.sorted

    keysList.foreach {
      f =>
        val p = orderedHashMap.get(f).get
        p.foreach { px =>
          finalPropertSet = finalPropertSet :+ px
        }
    }

    //    if( propertySet.contains(29) )
    //    println(propertySet.toString()+" -> "+finalPropertSet.toString())

    return finalPropertSet
  }
}
