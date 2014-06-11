package edu.nd.nina.wiki

import org.apache.spark.graphx._
import scala.collection.immutable.List

class WikiVertex(a: Double, b: Int, c: String, e: Boolean, f: Boolean, g: List[Msg], h: List[(VertexId,Double,Int)]) extends java.io.Serializable {
  def this(a1: Double, b1: Int, c1: String) = this(a1, b1, c1, false, false, List.empty, List.empty)
  def this(a1: Double, b1: Int, c1: String,  e: Boolean) = this(a1, b1, c1, e, false, List.empty, List.empty)
  def this(a1: Double, b1: Int, c1: String,  e: Boolean, f: Boolean) = this(a1, b1, c1, e, f, List.empty, List.empty)

  val dist: Double = a
  val ns: Int = b
  val title: String = c
  val col_msg: List[Msg] = g
  val tuples: List[(VertexId, Double,Int)] = h  
  val isDead = e
  val start: Boolean = f

  override def toString(): String = {
    var out:String  = dist + "\t" + ns + "\t" + title + "\t" 
    out
  }
  
  
}