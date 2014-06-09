package edu.nd.nina.wiki

import org.apache.spark.graphx._
import scala.collection.immutable.List

class WikiVertex(a: Double, b: Int, c: String, d: List[VertexId], e: Boolean, f: Boolean, g: List[Msg]) extends java.io.Serializable {
  def this(a1: Double, b1: Int, c1: String, d1: List[VertexId]) = this(a1, b1, c1, d1, false, false, List.empty)
  def this(a1: Double, b1: Int, c1: String, d1: List[VertexId], e: Boolean) = this(a1, b1, c1, d1, e, false, List.empty)
  def this(a1: Double, b1: Int, c1: String, d1: List[VertexId], e: Boolean, f: Boolean) = this(a1, b1, c1, d1, e, f, List.empty)

  val dist: Double = a
  val ns: Int = b
  val title: String = c
  var neighbours: List[VertexId] = d
  val col_msg: List[Msg] = g
  val isDead = e
  val start: Boolean = f

  override def toString(): String = {
    var out:String  = dist + "\t" + ns + "\t" + title + "\t" 
    for(x <- neighbours){
      out += x + ","
    }
    out
  }
  
  
}