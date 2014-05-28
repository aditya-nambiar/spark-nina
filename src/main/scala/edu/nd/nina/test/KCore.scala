package edu.nd.nina.test

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import scala.collection.immutable.HashSet

object KCore {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: KCore <master> <file>")
      System.exit(1)
    }

    val sc = new SparkContext(args(0), "KCore")
    var g = GraphLoader.edgeListFile(sc, args(1))
    val (numV, numE) = kCore(g)
    println("largest kCore: num vertices: " + numV + " numEdges: " + numE)
    sc.stop()
  }

  private def kCore(_g: Graph[Int, Int]): (Long, Long) = {
    var g = _g;
    val k = 3
    val maxIterationNo = 10
    var previousNumVertices = Long.MaxValue
    var currentNumVertices = g.numVertices
    var iterationNo = 1
    while (previousNumVertices != currentNumVertices && iterationNo < maxIterationNo) {
      println("iterationNo: " + iterationNo)
      previousNumVertices = currentNumVertices

      val connectedNodes = g.collectNeighbors(EdgeDirection.Out).mapValues((vid, vd) => vd.size).filter(v => if (v._2 >= k) true else false)

      val sc = g.vertices.context
      val vset = sc.broadcast(connectedNodes.collect().toSet)
      val newEdges = g.edges.filter(e =>
        !vset.value.filter(v =>
          v._1 == e.srcId)
          .isEmpty &&
          !vset.value.filter(v => v._1 == e.dstId).isEmpty)
      println(newEdges.count)
      val newerG = Graph(connectedNodes, newEdges)

      //debugRDD(g.vertices.collect, "vertices after filtering for the " + iterationNo + "th time")
      currentNumVertices = newerG.numVertices
      g = newerG
      iterationNo += 1
    }

    val ccGraph = g.ops.connectedComponents

    if (ccGraph.numVertices == 0) {
      println("No K Cores")
      return (0, 0)
    }

    val groupSize = ccGraph.vertices.map(
      v => (v._2, 1)).groupBy(
        (d) => d._1)

    val maxComponentIDAndSize = groupSize.reduce(
      (f, s) => {
        if (f._2.size > s._2.size) { f }
        else { s }
      })

    println("maxComponentID: " + maxComponentIDAndSize._1 + " componentSize: " + maxComponentIDAndSize._2.size)

    val t = ccGraph.subgraph(vpred =
      (v, d) => d == maxComponentIDAndSize._1)

    (t.numVertices, t.numEdges)

  }
}