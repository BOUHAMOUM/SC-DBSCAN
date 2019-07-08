package david.sc_dbscan.objects


import scala.collection.mutable.HashMap

object NodeBuilder {

  //  --------------------------------------------------------
  //  Create a node from the inputted data
  //  --------------------------------------------------------
  //TODO : devrait être dans la classe Noeud
  /**
    * Creates a node from a string (space separated property names).
    *
    * @param line space separated property names
    * @return the node
    */
  def createNode(line: String, useCoefficient: Boolean): Noeud = {
    var propertString = line.replace("Set", "").replace("(", "").replace(")", "").trim()
    var propertySet = propertString.replaceAll(" +", " ").split(" ")


    //  Get the node's id from the input file
    val noeudId = propertySet.head.toString

    if (!noeudId.equals("")) {
      var finalPropertySet: Set[Int] = Set()

      var coefficient: Int = 1

      if (useCoefficient) {
        try {
          coefficient = propertySet.last.toInt
          propertySet = propertySet.dropRight(1)
        } catch {
          case ioe: NoSuchElementException =>
          //        println(ioe)
          case e: Exception =>
          //        println(e)
        }
      }

      for (i <- 1 to propertySet.length - 1) {
        finalPropertySet = finalPropertySet.+(propertySet(i).toInt)
      }

      //    println(neudId+" - "+finalPropertySet.toSet)

      return new Noeud(noeudId.toInt, finalPropertySet, coefficient)
    }
    else return new Noeud(0, null, 0)


  }
}


class Noeud(id: Int, properties: Set[Int], coefficient: Int) extends Serializable {

  //  core define whether a node is a core or not
  private var core: Boolean = false

  //  The list of neighbors
  private var neighbors: HashMap[Int, Int] = HashMap()

  //  define whether a nose is visited or not yet during clustering stage
  private var visited: Boolean = false

  //TODO : inutile id est accessible directement
  def getId(): Int = this.id

  //TODO : idem id
  def getProperties(): Set[Int] = this.properties

  def setCore() {
    this.core = true
  }

  def isCore(): Boolean = this.core

  //TODO : définir l'accès à _isCore
  //  def isCore = _isCore
  //  def isCore_= (newValue: Boolean): Unit = { _isCore = newValue }

  //TODO : idem _isCore (_wasVisited)
  def setVisited() {
    this.visited = true
  }

  def setNotVisited() {
    this.visited = false
  }

  def isVisited(): Boolean = this.visited

  def addNeighbor(noaudId: Int, coefficient: Int) {
    this.neighbors.put(noaudId, coefficient)
  }

  //  def addAllNeighbor(noauds: Array[String]) {
  //    this.neighbors = this.neighbors ++ (noauds)
  //  }

  def getNeighbors(): HashMap[Int, Int] = this.neighbors

  def setNeighborList(neighbors: HashMap[Int, Int]) {
    this.neighbors = neighbors
  }

  def getCoefficient(): Int = {

    return this.coefficient
  }

  //  Override the comparison function
  //  Two nodes are equals if they have the same ID

  def canEqual(a: Any): Boolean = a.isInstanceOf[Noeud]

  override def equals(that: Any): Boolean =
    that match {
      case that: Noeud => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }

  override def hashCode: Int = this.id.hashCode()


  override def toString = s"Noeud($id)"
}
