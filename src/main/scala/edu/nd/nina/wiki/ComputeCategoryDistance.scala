package edu.nd.nina.wiki

import org.apache.spark.graphx.Graph
import org.apache.spark.graphx._
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.Logging
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.List
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
    println("starting vertex id " + rvid)
    val temp = cleanG.mapTriplets(x => if (x.srcAttr.ns == 0 && x.dstAttr.ns == 0) -1.0 else 1.0)

    val (tp1, tp2) = sssp(temp, rvid)

    temp
  }

  def sssp(g: Graph[WikiVertex, Double], src: VertexId): (Graph[WikiVertex, Double], Double) = {

    var init = g.mapVertices((id, x) => if (id == src) { new WikiVertex(0, x.ns, x.title, x.neighbours) } else { new WikiVertex(Double.PositiveInfinity, x.ns, x.title, x.neighbours) })

    def vertexProgram(src: VertexId, oldDist: WikiVertex, recmsgs: List[Msg]): WikiVertex =
      {
        println("Executing on vertex")
        if (oldDist.dist != Double.PositiveInfinity) {
          return oldDist
        }

        if (oldDist.ns == 0) { //Article
          var mini: Double = Double.PositiveInfinity
          val min = recmsgs.reduce((x,y) => if (x.dist < y.dist)  x else  y)

          return new WikiVertex(min.dist + 1, oldDist.ns, oldDist.title, oldDist.neighbours)

        } else { //Category

          var temp_msg_buff = List.empty[Msg]
          var temp_node = new WikiVertex(Double.PositiveInfinity, oldDist.ns, oldDist.title, oldDist.neighbours)
          recmsgs.foreach(x =>
            if (x.dist != Double.PositiveInfinity && x.dist > -1) {
              var newmsg = new Msg(x.to, x.dist + 1)
              temp_msg_buff = newmsg :: temp_msg_buff
            })
          temp_node.col_msg = temp_msg_buff

          return temp_node
        }

      }

    def sendMessage(edge: EdgeTriplet[WikiVertex, Double]) =
      {
        var i = 0
        if (edge.srcAttr.ns == 0 && edge.dstAttr.ns == 14) { // Article to Category
          for (i <- 1 to edge.srcAttr.neighbours.length) {
            var tp = new Msg(edge.srcAttr.neighbours(i), edge.srcAttr.dist)
            var ty = tp :: List.empty[Msg]
            Iterator((edge.dstId, ty))
          }
          
        }
        if (edge.srcAttr.ns == 14 && edge.dstAttr.ns == 14) { // Category to Category
        
          Iterator((edge.dstId, edge.srcAttr.col_msg))

        }
        if (edge.srcAttr.ns == 14 && edge.dstAttr.ns == 0) { // Category to Article
          var temp_msg_buf = List.empty[Msg]
          edge.srcAttr.col_msg.foreach(x =>
            if (edge.dstId == x.to) {
              temp_msg_buf = x :: temp_msg_buf
            })
          Iterator((edge.dstId, temp_msg_buf))

        } else {
          Iterator.empty //Article to article edge
        }

      }

    def messageCombiner(a: List[Msg], b: List[Msg]): List[Msg] = {
      val c = a ::: b
      val d = c.groupBy(x => x.to)
      val e = d.map(x => x._2.reduce((a, b) => if (a.dist < b.dist) a else b))
      e.toList
    }
    // The initial message received by all vertices in PageRank

    val initialMessage =  List[Msg]()
    val nmsg = new Msg(1L, Double.PositiveInfinity)
    val initMsg = nmsg :: initialMessage

    val sssp = init.pregel(initMsg)(
      vertexProgram,
      sendMessage,
      messageCombiner)

    println("----------------------------------------------")
    var summed = 1.0
    /* val tp1 = sssp.vertices.collect
    for ((x, y) <- tp1) { println(y.title + " " + y.dist) }
    var summed = sssp.vertices.map((a) => a._2.dist).reduce(math.max(_, _))
*/
    (sssp, summed)
  }

  def main(args: Array[String]) {

  }
}
class Msg(a: VertexId, b: Double) extends Serializable{
  var to = a
  var dist = b

}
