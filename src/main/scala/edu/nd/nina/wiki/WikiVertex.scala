package edu.nd.nina.wiki

import org.apache.spark.graphx._
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.List


class WikiVertex(a: Double, b: Int, c: String, d: Array[VertexId],e :Boolean,f:Boolean) {
  def this(a1: Double, b1: Int, c1: String, d1: Array[VertexId]) = this(a1,b1,c1,d1, false,false) 
  def this(a1: Double, b1: Int, c1: String, d1: Array[VertexId], e :Boolean) = this(a1,b1,c1,d1, e,false) 
  
  var dist: Double = a
  var ns: Int = b
  var title: String = c
  var neighbours : Array[VertexId] = d
  var col_msg : List[Msg] = List.empty[Msg]
  var isDead = e
  var start: Boolean = f
  
  
  
  


}