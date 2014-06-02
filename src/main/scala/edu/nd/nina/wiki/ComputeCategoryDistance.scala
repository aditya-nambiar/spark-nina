package edu.nd.nina.wiki

import org.apache.spark.graphx.Graph
import org.apache.spark.graphx._
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.Logging
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.List
import scala.collection.mutable.Set
object ComputeCategoryDistance extends Logging {
    var fin_art1 :collection.mutable.Set[VertexId] = Set.empty
    var fin_art2 :collection.mutable.Set[VertexId] = Set.empty
    var use2: Boolean = false
    var clean_flag = true
    var msg_flow : Int = 0

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
        if(clean_flag == true){//first VP after SM
          if(use2==false ){//fin_art2 was used earlier use it
            fin_art1.clear() //and store in fin_art1
            
          }
          else{
            fin_art2.clear()
          }
          use2 = !use2
          clean_flag=false
          println("Messages in flow :" + msg_flow )
          msg_flow =0
        }
        //println("Executing on vertex")
        if (oldDist.dist != Double.PositiveInfinity && oldDist.dist !=0 ) {
          if(use2==true)
           fin_art1 += src
          else
           fin_art2 +=src
          return new WikiVertex(oldDist.dist,oldDist.ns,oldDist.title,oldDist.neighbours,true)
        }
        if(oldDist.dist ==0){
          
          if(oldDist.start==false)
            return new WikiVertex(oldDist.dist,oldDist.ns,oldDist.title,oldDist.neighbours,false,true)
          else{
            if(use2==true)
            fin_art1 += src
            else
              fin_art2+=src
            return new WikiVertex(oldDist.dist,oldDist.ns,oldDist.title,oldDist.neighbours,true,true)
          }
            
        }
      
        if (oldDist.ns == 0) { //Article
         // var mini: Double = Double.PositiveInfinity
          val min = recmsgs.reduce((x, y) => if (x.dist < y.dist) x else y)
          if(min.dist.isInfinite()){return oldDist}
                    println(oldDist.title + " " + min.dist)

          return new WikiVertex(min.dist + 1, oldDist.ns, oldDist.title, oldDist.neighbours)

        } else { //Category

          var temp_msg_buff = List.empty[Msg]
          var temp_node = new WikiVertex(Double.PositiveInfinity, oldDist.ns, oldDist.title, oldDist.neighbours)
          recmsgs.foreach(x =>
            if (x.dist != Double.PositiveInfinity && x.dist > -1) {
              if(x.d_ac < 6 ){
                  if (use2 ==true && !fin_art2.contains(x.to))
	              {var newmsg = new Msg(x.to, x.dist + 1,x.d_ac +1)
	              temp_msg_buff = newmsg :: temp_msg_buff
	             
	              }
                  else if (use2 == false && !fin_art1.contains(x.to))
                  {
                  var newmsg = new Msg(x.to, x.dist + 1,x.d_ac +1)
	              temp_msg_buff = newmsg :: temp_msg_buff
	              
                  }
              }
            })
            msg_flow = msg_flow + temp_msg_buff.length
          //  println("tp "+ temp_msg_buff.length+ " " + src)
          temp_node.col_msg = temp_msg_buff

          return temp_node
        }

      }

    def sendMessage(edge: EdgeTriplet[WikiVertex, Double]): Iterator[(VertexId, List[Msg])] =
      {
        clean_flag = true
        if (edge.srcAttr.ns == 0 && edge.dstAttr.ns == 14) { // Article to Category
          if(edge.srcAttr.isDead==true){
            Iterator.empty
          }
          else{
              
	          if (edge.srcAttr.dist.isInfinity) {
	            Iterator.empty
	          } else {
	            var i =  List.empty[Msg]
	            for (n <- edge.srcAttr.neighbours) {
	              var tp = new Msg(n, edge.srcAttr.dist)
	              i = tp :: i 
	            }
	            var tp = new Msg(edge.srcId,edge.srcAttr.dist)
	            i = tp:: i
	           
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
            if (edge.dstId == x.to ) {
              temp_msg_buf = x :: temp_msg_buf
            })
          if (temp_msg_buf.isEmpty || edge.dstAttr.isDead==true) {
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

    val sssp = init.pregel(initMsg)(
      vertexProgram,
      sendMessage,
      messageCombiner)

    println("----------------------------------------------")
    
    var summed = 1.0
     val tp1 = sssp.vertices.collect
    for ((x, y) <- tp1) { println(y.title + " " + y.dist) }
   // var summed = sssp.vertices.map((a) => a._2.dist).reduce(math.max(_, _))

    (sssp, summed)
  }

  def main(args: Array[String]) {

  }
}
class Msg(a: VertexId, b: Double, c : Int) extends Serializable {
  
  def this(a: VertexId, b: Double) = this(a,b,0)
  var to = a
  var dist = b
  var d_ac : Int = c
  
}
