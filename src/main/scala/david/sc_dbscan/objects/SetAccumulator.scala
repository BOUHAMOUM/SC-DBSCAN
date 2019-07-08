package david.sc_dbscan.objects

import org.apache.spark.AccumulatorParam

object SetAccumulator extends AccumulatorParam[Set[String]] {
  def zero(initialValue: Set[String]): Set[String] = {
    return initialValue
  }

  def addInPlace(v1: Set[String], v2: Set[String]): Set[String] = {
    v1.++:(v2)
  }
}