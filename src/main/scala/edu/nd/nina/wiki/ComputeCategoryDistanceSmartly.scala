package edu.nd.nina.wiki

import org.apache.spark.graphx.Graph
import org.apache.spark.graphx._
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.Logging
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.List
import scala.collection.immutable.Set
import scala.collection.immutable.Map
import edu.nd.nina.test.ApproxDiameter
import edu.nd.nina.test.MyPregel
import org.apache.spark.Accumulator
import org.apache.spark.broadcast.Broadcast

object ComputeCategoryDistanceSmartly extends Logging {
  //  var fin_art1: collection.mutable.Set[VertexId] = Set.empty
  //  var fin_art2: collection.mutable.Set[VertexId] = Set.empty
  //  var use2: Boolean = false
  //  var clean_flag = true
  //  var msg_flow: Int = 0
  //  var killed: Int = 0

  def compute(g: Graph[WikiVertex, Double], rvid: VertexId, bcstNbrMap:Broadcast[Map[VertexId, Set[VertexId]]]): Graph[WikiVertex, Double] = {
    
    
    println("starting vertex id " + rvid)
    

    val (tp1, tp2) = sssp(g, rvid, bcstNbrMap)

    tp1
  }

  def sssp(g: Graph[WikiVertex, Double], src: VertexId, bcstNbrMap:Broadcast[Map[VertexId, Set[VertexId]]]): (Graph[WikiVertex, Double], Double) = {

    var init = g.mapVertices((id, x) => if (id == src) { new WikiVertex(0, x.ns, x.title) } else { new WikiVertex(Double.PositiveInfinity, x.ns, x.title) })

    def vertexProgram(src: VertexId, oldDist: WikiVertex, recmsgs: List[(VertexId, Double, Int)]): WikiVertex =
      {
         if(recmsgs(0)._1 == 1) {return oldDist}
        if (oldDist.dist != Double.PositiveInfinity && oldDist.dist != 0) {
          //not start
          return new WikiVertex(oldDist.dist, oldDist.ns, oldDist.title, true)
        }
        if (oldDist.dist == 0) {

          if (oldDist.start == false)
            return new WikiVertex(oldDist.dist, oldDist.ns, oldDist.title, false, true)
          else {
            return new WikiVertex(oldDist.dist, oldDist.ns, oldDist.title, true, true)
          }

        }
        if (oldDist.ns == 0) { //Article
          val min = recmsgs.reduce((x, y) => if (x._2 < y._2) x else y)
          if (min._2.isInfinite()) { return oldDist }
          println("Ya " + oldDist.title + " " + (min._2 + 1d))

          return new WikiVertex(min._2 + 1, oldDist.ns, oldDist.title)

        } else { //Category

          val recmsgFilter = recmsgs.filter(x => if (x._2 != Double.PositiveInfinity ) true else false)
          val recmsgM = recmsgFilter.map(x => (x._1, x._2+1, x._3+1))

          return new WikiVertex(Double.PositiveInfinity, oldDist.ns, oldDist.title, false, false, List.empty, recmsgM,0)
        }

      }

    def sendMessage(edge: EdgeTriplet[WikiVertex, Double]): Iterator[(VertexId, List[(VertexId, Double, Int)] )] =
      {

        if (edge.srcAttr.ns == 0 && edge.dstAttr.ns == 14) { // Article to Category
          if (edge.srcAttr.isDead == true) {
            Iterator.empty
          } else {

            if (edge.srcAttr.dist.isInfinity) {
              Iterator.empty
            } else {
              
              Iterator((edge.dstId, List((edge.srcId, edge.srcAttr.dist, 0)) ))
            }
          }

        } else if (edge.srcAttr.ns == 14 && edge.dstAttr.ns == 14) { // Category to Category
          
          if (edge.srcAttr.tuples.isEmpty) {
            Iterator.empty
          } else {
            Iterator((edge.dstId, edge.srcAttr.tuples))
          }

        } else if (edge.srcAttr.ns == 14 && edge.dstAttr.ns == 0) { // Category to Article
          if(edge.dstAttr.isDead == true){
            Iterator.empty
          }
          var temp_msg_buf = List.empty[(VertexId, Double, Int)]         
         
          edge.srcAttr.tuples.foreach(x =>
            if(bcstNbrMap.value(edge.dstId).contains(x._1)){
              //x.last_cat = edge.srcId
              temp_msg_buf = x :: temp_msg_buf
            })
          if (temp_msg_buf.isEmpty) {
            Iterator.empty
          } else {
            Iterator((edge.dstId, temp_msg_buf))
          }

        } else {
          Iterator.empty //Article to article edge
        }

      }

    def messageCombiner(a: List[(VertexId, Double,Int)], b: List[(VertexId, Double, Int)]): List[(VertexId, Double,Int)] = {
      val c = a ::: b
      val d = c.groupBy(x => x._1)
      val e = d.map(x => x._2.reduce((a, b) => if (a._2 < b._2) a else b))
      e.toList
    }
    // The initial message received by all vertices in PageRank

    val initialMessage = List[(VertexId, Double,Int)]()
    val nmsg = (1L, Double.PositiveInfinity, 0)
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