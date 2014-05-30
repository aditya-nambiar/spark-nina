package edu.nd.nina.wiki

import org.apache.spark.graphx._
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.List

class WikiVertex(a: Double, b: Int, c: String, d: Array[VertexId]) {
  var dist: Double = a
  var ns: Int = b
  var title: String = c
  var neighbours : Array[VertexId] = d
  var col_msg : List[Msg] = List.empty[Msg]

}