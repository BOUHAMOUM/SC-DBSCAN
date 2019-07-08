package david.sc_dbscan.process

import david.sc_dbscan.objects.{NodeBuilder, Noeud, Partition}
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}

class PartitioningSpec extends FlatSpec with Matchers {
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("PartitioningSpec")
      .getOrCreate()
  }

  "A node" should "be built from property names" in {
    val propertyNames = "2 b c a e"
    val properties = Set(2, 3, 1, 5)
    val expected = new Noeud(2, properties, 1)

    val result = NodeBuilder.createNode(propertyNames, false)

    result should be (expected)
    result.getProperties() should be (properties)
  }

  "A node" should "belong to the correct partitions" in {
    val properties = Set(2, 3, 1, 5)
    val node = new Noeud(2, properties, 1)
    val expected = new Array[Partition](4)
    expected(0) = new Partition(Set(2))
    expected(0).addNoeud(node)
    expected(1) = new Partition(Set(3))
    expected(1).addNoeud(node)
    expected(2) = new Partition(Set(1))
    expected(2).addNoeud(node)
    expected(3) = new Partition(Set(5))
    expected(3).addNoeud(node)

    val order: Map[Int, Int] = Map()

    val result = Partitioning.affliate(node, node.getProperties(), 0.8, order)

    result should equal (expected)
  }

  "A partition" should "contains the right nodes" in {
    val data = Array("2 b c a e", "3 b c e")
    val rdd = spark.sparkContext.parallelize(data)
    val node2 = new Noeud(2, Set(2, 3, 1, 5), 1)
    val node3 = new Noeud(3, Set(2, 3, 5), 1)
    val expected = Array(
      (Set("a"), Array(node2)),
      (Set("b"), Array(node2, node3)),
      (Set("c"), Array(node2, node3)),
      (Set("e"), Array(node2, node3))
    )

    val order: Map[Int, Int] = Map()

    val result = Partitioning.getInitalPartition(rdd, 0.8, false, order).sortBy(t => t._1.mkString).collect()

    //result(0)._1 should equal (expected(0)._1)
   // result(0)._2 should equal (expected(0)._2)
  }
}
