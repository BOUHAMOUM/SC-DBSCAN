package david.sc_dbscan.objects

class ClusterId (id: Set[String]) extends Serializable {

  def getId(): Set[String] = {
    return id
  }

  def canEqual(a: Any) = a.isInstanceOf[ClusterId]

//  override def equals(that: Any): Boolean =
//    that match {
//      case that: ClusterId => that.canEqual(this) && !this.getId().intersect(that.getId()).isEmpty
//      case _               => false
//    }
//
//  override def hashCode: Int = {
//
//    return 1
//  }

  override def equals(that: Any): Boolean =
    that match {
      case that: ClusterId => that.canEqual(this) && !this.getId().intersect(that.getId()).isEmpty
      case _               => false
    }

  override def hashCode: Int = {

//    val x = this.id.hashCode() % 4
//
//    println(this.getId().toString()+" -> "+x.toString)

    return scala.math.abs(this.id.hashCode()) % 10
  }


  override def toString(): String = {
    return this.id.toString()
  }
}