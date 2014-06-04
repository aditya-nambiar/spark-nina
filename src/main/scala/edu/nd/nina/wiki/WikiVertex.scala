package edu.nd.nina.wiki

import org.apache.spark.graphx._
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.List

class WikiVertex(a: Double, b: Int, c: String, d: Array[VertexId], e: Boolean, f: Boolean, g: List[Msg]) {
  def this(a1: Double, b1: Int, c1: String, d1: Array[VertexId]) = this(a1, b1, c1, d1, false, false, List.empty)
  def this(a1: Double, b1: Int, c1: String, d1: Array[VertexId], e: Boolean) = this(a1, b1, c1, d1, e, false, List.empty)
  def this(a1: Double, b1: Int, c1: String, d1: Array[VertexId], e: Boolean, f: Boolean) = this(a1, b1, c1, d1, e, f, List.empty)

  val dist: Double = a
  val ns: Int = b
  val title: String = c
  var neighbours: Array[VertexId] = d
  val col_msg: List[Msg] = g
  val isDead = e
  val start: Boolean = f

}