package david.sc_dbscan.process

import david.sc_dbscan.objects.Noeud

object Compare {
  
  def similar(n1: Noeud, n2: Noeud, eps:Double): Boolean = {
    
    var intersection:Double = n1.getProperties().toSet.intersect(n2.getProperties().toSet).size
    var union:Double = n1.getProperties().toSet.union(n2.getProperties().toSet).size
    
    var cpt:Double = intersection / union

//    println(n1+" "+n2+" = "+cpt)

    if( cpt >= eps )
      return true
    else return false
  }

  def similar(n1: Noeud, n2: Set[Int], eps:Double): Boolean = {

    var intersection:Double = n1.getProperties().toSet.intersect(n2).size
    var union:Double = n1.getProperties().toSet.union(n2).size

    var cpt:Double = intersection / union

    //    println(n1+" "+n2+" = "+cpt)

    if( cpt >= eps )
      return true
    else return false
  }
}