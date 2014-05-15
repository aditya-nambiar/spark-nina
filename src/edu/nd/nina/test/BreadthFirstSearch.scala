package edu.nd.nina.test

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._
import scala.reflect.ClassTag

object BreadthFirstSearch {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: ApproxDiameter <master> <file>")
      System.exit(1)
    }

    val sc = new SparkContext(args(0), "ApproxDiameter")
    var g = GraphLoader.edgeListFile(sc, args(1)).mapVertices((v,d) => d.toDouble)
    val (h, max) = sssp(g, 1L)
    println(h.vertices.collect.mkString("\n"))
    println("Max: " + max)
    sc.stop()
  }

  def sssp(g: Graph[Double, Int], src: VertexId): (Graph[Double, Int], Int) = {

    val initGraph = g.mapVertices((id, _) => if (id == src) 0 else Double.PositiveInfinity)
    def vertexProgram(src: VertexId, dist: Double, newDist: Double): Double =
      math.min(dist, newDist)
    def sendMessage(edge: EdgeTriplet[Double, Int]) =
      if (edge.srcAttr + edge.attr < edge.dstAttr) {
        Iterator((edge.dstId, edge.srcAttr + edge.attr))
      } else {
        Iterator.empty
      }
    def messageCombiner(a: Double, b: Double): Double = {
      math.min(a, b)
    }
    // The initial message received by all vertices in PageRank
    val initialMessage = Double.PositiveInfinity

    val sssp = initGraph.pregel(initialMessage)(
      vertexProgram,
      sendMessage,
      messageCombiner)
    
      
    var summed = sssp.vertices.map((a) => a._2).reduce(math.max(_,_))
    
    if(summed.isInfinity){
      println("Source Node is Sink")
      summed = 0
    }
    
    (sssp,summed.toInt)
  }
}
