
package david.sc_dbscan.objects

import org.apache.spark.AccumulatorParam

import scala.collection.mutable
import scala.collection.mutable.HashMap

object HashMapAccumulator extends AccumulatorParam[HashMap[ClusterId, Set[Noeud]]] {

  def zero(initialValue: HashMap[ClusterId, Set[Noeud]]) = HashMap()

  def addInPlace(v1: HashMap[ClusterId, Set[Noeud]], v2: HashMap[ClusterId, Set[Noeud]]): HashMap[ClusterId, Set[Noeud]] = {
    v1.++:(v2)
  }
}