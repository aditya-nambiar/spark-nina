package edu.nd.nina.wiki

import org.apache.spark.graphx.Graph
import org.apache.spark.graphx._
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.Logging
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.List
import scala.collection.mutable.Set
import edu.nd.nina.test.ApproxDiameter
import edu.nd.nina.test.MyPregel
object ComputeCategoryDistance extends Logging {
  //  var fin_art1: collection.mutable.Set[VertexId] = Set.empty
  //  var fin_art2: collection.mutable.Set[VertexId] = Set.empty
  //  var use2: Boolean = false
  //  var clean_flag = true
  //  var msg_flow: Int = 0
  //  var killed: Int = 0

  def compute(g: Graph[WikiVertex, Double], rvid: VertexId): Graph[WikiVertex, Double] = {

    println("starting vertex id " + rvid)

    val (tp1, tp2) = sssp(g, rvid)

    tp1
  }

  def sssp(g: Graph[WikiVertex, Double], src: VertexId): (Graph[WikiVertex, Double], Double) = {

    var init = g.mapVertices((id, x) => if (id == src) { new WikiVertex(0, x.ns, x.title, x.neighbours) } else { new WikiVertex(Double.PositiveInfinity, x.ns, x.title, x.neighbours) })

    def vertexProgram(src: VertexId, oldDist: WikiVertex, recmsgs: List[Msg]): WikiVertex =
      {

        if (oldDist.dist != Double.PositiveInfinity && oldDist.dist != 0) {

          return new WikiVertex(oldDist.dist, oldDist.ns, oldDist.title, oldDist.neighbours, true)
        }
        if (oldDist.dist == 0) {

          if (oldDist.start == false)
            return new WikiVertex(oldDist.dist, oldDist.ns, oldDist.title, oldDist.neighbours, false, true)
          else {

            return new WikiVertex(oldDist.dist, oldDist.ns, oldDist.title, oldDist.neighbours, true, true)
          }

        }
        if (oldDist.ns == 0) { //Article
          val min = recmsgs.reduce((x, y) => if (x.dist < y.dist) x else y)
          if (min.dist.isInfinite()) { return oldDist }
          println("Ya " + oldDist.title + " " + min.dist)

          return new WikiVertex(min.dist + 1, oldDist.ns, oldDist.title, oldDist.neighbours)

        } else { //Category

          val recmsgFilter = recmsgs.filter(x => if (x.dist != Double.PositiveInfinity && x.d_ac < 6) true else false)
          val recmsgM = recmsgFilter.map(x => new Msg(x.to, x.dist + 1, x.d_ac + 1))

          return new WikiVertex(Double.PositiveInfinity, oldDist.ns, oldDist.title, oldDist.neighbours, false, false, recmsgM)
        }

      }

    def sendMessage(edge: EdgeTriplet[WikiVertex, Double]): Iterator[(VertexId, List[Msg])] =
      {

        if (edge.srcAttr.ns == 0 && edge.dstAttr.ns == 14) { // Article to Category

          if (edge.srcId == 12 || edge.dstId == 12) {
            println("12s")
          }

          if (edge.srcAttr.isDead == true) {
            Iterator.empty
          } else {

            if (edge.srcAttr.dist.isInfinity) {
              Iterator.empty
            } else {
              var i = List.empty[Msg]
              for (n <- edge.srcAttr.neighbours) {
                var tp = new Msg(n, edge.srcAttr.dist)
                i = tp :: i
              }
              var tp = new Msg(edge.srcId, edge.srcAttr.dist, 4)
              i = tp :: i

              Iterator((edge.dstId, i))
            }
          }

        } else if (edge.srcAttr.ns == 14 && edge.dstAttr.ns == 14) { // Category to Category
          if (edge.srcAttr.col_msg.isEmpty) {
            Iterator.empty
          } else {
            Iterator((edge.dstId, edge.srcAttr.col_msg))
          }

        } else if (edge.srcAttr.ns == 14 && edge.dstAttr.ns == 0) { // Category to Article
          var temp_msg_buf = List.empty[Msg]
          edge.srcAttr.col_msg.foreach(x =>
            if (edge.dstId == x.to) {
              temp_msg_buf = x :: temp_msg_buf
            })
          if (temp_msg_buf.isEmpty || edge.dstAttr.isDead == true) {
            Iterator.empty
          } else {
            Iterator((edge.dstId, temp_msg_buf))
          }

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

    val initialMessage = List[Msg]()
    val nmsg = new Msg(1L, Double.PositiveInfinity)
    val initMsg = nmsg :: initialMessage

    val sssp = MyPregel(init, initMsg, Int.MaxValue, EdgeDirection.Out)(
      vertexProgram,
      sendMessage,
      messageCombiner)

    println("----------------------------------------------")

    var summed = 1.0
    val tp1 = sssp.vertices.collect
    for ((x, y) <- tp1) {
      if (x > 0)
        println(y.title + " " + y.dist)
    }
    // var summed = sssp.vertices.map((a) => a._2.dist).reduce(math.max(_, _))

    (sssp, summed)
  }

  def main(args: Array[String]) {

  }
}
class Msg(a: VertexId, b: Double, c: Double) extends Serializable {

  def this(a: VertexId, b: Double) = this(a, b, 0)
  var to = a
  var dist = b
  var d_ac: Double = c

}
