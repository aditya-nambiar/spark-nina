package edu.nd.nina.wiki

import org.apache.spark.graphx.Graph
import org.apache.spark.graphx._
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.Logging

object ComputeCategoryDistance extends Logging {
  var global: Graph[WikiVertex, Double] = _

  def compute(g: Graph[WikiVertex, Double]): Graph[WikiVertex, Double] = {
    println("yo " + g.vertices.count)
    logWarning("Graph has %d vertex partitions, %d edge partitions".format(g.vertices.partitions.length, g.edges.partitions.length))
    logWarning(s"DIRTY graph has ${g.triplets.count()} EDGES, ${g.vertices.count()} VERTICES")

    // TODO: try reindexing
    val cleanG = g.subgraph(x => true,
      (vid, vd) => vd != null).partitionBy(PartitionStrategy.EdgePartition2D).cache()
    cleanG.vertices.setName("cleanG vertices")
    cleanG.edges.setName("cleanG edges")

    logWarning(s"ORIGINAL graph has ${cleanG.triplets.count()} EDGES, ${cleanG.vertices.count()} VERTICES")
    logWarning(s"CLEAN graph has ${cleanG.triplets.count()} EDGES, ${cleanG.vertices.count()} VERTICES")
    println("CLean Graph")
    // val rt= cleanG.vertices.collect
    // for( (x,y) <- rt ){println(y.ns)}
    //  val tp4=cleanG.edges.collect
    // for(x <- tp4){println(x.srcId+ " -> "+ x.dstId+ " Weight " + x.attr)  }
    //   val (rvid, rvd) = ApproxDiameter.pickRandomVertex[String,Double](cleanG)
    val rvid = cleanG.vertices.first._1
    val temp = cleanG.mapTriplets(x => if (x.srcAttr.ns == 0 && x.dstAttr.ns == 0) -1.0 else 1.0)

    val (tp1, tp2) = sssp(temp, rvid)

    temp
  }

  def sssp(g: Graph[WikiVertex, Double], src: VertexId): (Graph[WikiVertex, Double], Double) = {

    val initGraph = g.mapVertices((id, x) => if (id == src) { new WikiVertex(0, x.ns, x.title) } else { new WikiVertex(Double.PositiveInfinity, x.ns, x.title) })
    val a = g
    val b = 6
    global = g
    def vertexProgram(src: VertexId, oldDist: WikiVertex, newDist: Double): WikiVertex =
      {
        val c = math.min(oldDist.dist, newDist)
        val r = new WikiVertex(c, oldDist.ns, oldDist.title)
        r

      }

    def sendMessage(edge: EdgeTriplet[WikiVertex, Double]) =
      {

        if (edge.srcAttr.dist != Double.PositiveInfinity) {
          if (edge.srcAttr.ns == 0 && edge.dstAttr.ns == 0) {
            if (edge.attr == -1) {

              val src_in = edge.srcId
              val dst_in = edge.dstId
              val source = edge.srcAttr
              val dest = edge.dstAttr
              val initGraph_in = global.mapVertices((id, y) => if (id == src_in) { new WikiVertex(0, y.ns, y.title) } else { new WikiVertex(Double.PositiveInfinity, y.ns, y.title) })

              def vertexProgram_in(src: VertexId, oldDist: WikiVertex, newDist: Double): WikiVertex = {

                val c = math.min(oldDist.dist, newDist)
                val r = new WikiVertex(c, oldDist.ns, oldDist.title)
                r

              }

              def sendMessage_in(edge_in: EdgeTriplet[WikiVertex, Double]) = {

                if (edge_in.srcAttr.dist + edge_in.attr < edge_in.dstAttr.dist &&
                  ((edge_in.srcAttr.ns == 14 && edge_in.dstAttr.ns == 14) || (edge_in.srcId == src_in && edge_in.dstAttr.ns == 14) || (edge_in.dstId == dst_in && edge_in.srcAttr.ns == 14))) {

                  Iterator((edge_in.dstId, edge_in.srcAttr.dist + edge_in.attr))

                } else {
                  Iterator.empty
                }

              }
              def messageCombiner_in(a: Double, b: Double): Double = {
                math.min(a, b)
              }

              // The initial message received by all vertices in PageRank
              val initialMessage_in = Double.PositiveInfinity

              val sssp_in = initGraph_in.pregel(initialMessage_in)(
                vertexProgram_in,
                sendMessage_in,
                messageCombiner_in)
              //Better method to find attr given id??
              var x = 4.0

              val v = sssp_in.vertices.filter(c => c._1 == dst_in)

              println(source.title + " " + dest.title + " " + v.first._2.dist)
              edge.attr = v.first._2.dist
            }

            if (edge.srcAttr.dist + edge.attr < edge.dstAttr.dist) {

              Iterator((edge.dstId, edge.srcAttr.dist + edge.attr))

            } else { Iterator.empty }
          } else {
            Iterator.empty
          }

        } else {
          Iterator.empty
        }
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

    println("----------------------------------------------")
    val tp1 = sssp.vertices.collect
    for ((x, y) <- tp1) { println(y.title + " " + y.dist) }
    var summed = sssp.vertices.map((a) => a._2.dist).reduce(math.max(_, _))

    (sssp, summed)
  }

  def main(args: Array[String]) {

  }
}