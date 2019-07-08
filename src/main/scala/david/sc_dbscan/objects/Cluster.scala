package david.sc_dbscan.objects

//  Inthe class cluster have an ID and a list of nodes
class Cluster (id: Set[String]) extends Serializable {

  private var listNoeud: Array[Noeud] = Array();

  def addNoeud(noeud: Noeud) {
    listNoeud = listNoeud :+ noeud
  }

  def addAllNoeuds(noeuds: Array[Noeud]) {
    listNoeud = listNoeud.++:(noeuds)
  }

  def getNoeuds(): Array[Noeud] = {
    return listNoeud
  }

  def getId(): Set[String] = {
    return this.id
  }

  override def toString(): String = {
    return this.id.toString()
  }

//  def canEqual(a: Any) = a.isInstanceOf[Cluster]
//
//  override def equals(that: Any): Boolean =
//    that match {
//      case that: ClusterId => that.canEqual(this) && this.hashCode == that.hashCode
//      case _               => false
//    }
//
//  override def hashCode: Int = {
//
//    return this.id.hashCode()
//  }
}