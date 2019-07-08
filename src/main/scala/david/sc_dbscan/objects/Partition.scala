package david.sc_dbscan.objects

import david.sc_dbscan.process.Compare

class Partition (id: Set[Int]) extends Serializable {
  private var listNoeud: Array[Noeud] = Array();

  def addNoeud(noeud: Noeud) {
    listNoeud = listNoeud :+ (noeud)
  }

  def addNoeuds(noeuds: Array[Noeud]) {
    for (i <- 0 to noeuds.length - 1) {
      listNoeud = listNoeud :+ (noeuds(i))
    }
  }

  def addDuplicatedNoeuds(noeuds: Array[Noeud], id: Set[Int], eps: Double) {
    for (i <- 0 to noeuds.length - 1) {
      if ( Compare.similar(noeuds(i), id, eps)  )
        {
          listNoeud = listNoeud :+ (noeuds(i))
        }
    }
  }

  def getListNoeud(): Array[Noeud] = {
    return listNoeud
  }

  def getId(): Set[Int] = {
    return id
  }

  def samePartition(p: Partition): Boolean = {

    if (this.getId() == p.getId())
      return true

    else return false

  }

  override def toString = s"Partition($getId, ${listNoeud.mkString(", ")})"

  def canEqual(other: Any): Boolean = other.isInstanceOf[Partition]

  override def equals(other: Any): Boolean = other match {
    case that: Partition =>
      (that canEqual this) &&
      getId() == that.getId() &&
      (listNoeud sameElements that.listNoeud)
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(listNoeud)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}