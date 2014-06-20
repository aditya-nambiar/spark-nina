package edu.nd.nina.wiki
import org.apache.spark.graphx._
import edu.nd.nina.test.MyPregel
object bfs_articles {

  def compute(g: Graph[WikiVertex, Double], rvid: VertexId,title: String): Graph[WikiVertex, Double] = {

    println("starting vertex id " + rvid)

    val (tp1, tp2) = bfs(g, rvid,title)

    tp1
  }
  
  def bfs(g: Graph[WikiVertex, Double], src: VertexId,title:String): (Graph[WikiVertex, Double], Double) = {
  
    var init = g.mapVertices((id, x) => if (id == src) { new WikiVertex(0, x.ns, x.title) } else { new WikiVertex(Double.PositiveInfinity, x.ns, x.title) })

    def vertexProgram(src: VertexId, oldDist: WikiVertex, recmsgs: Double): WikiVertex =
      {
      
        
        if (oldDist.ns == 0 && recmsgs != Double.PositiveInfinity) { //Article
       
          println(recmsgs+" "+src)
          return new WikiVertex(recmsgs, oldDist.ns, oldDist.title)

        }
        else{
          return oldDist
        }
      }

    def sendMessage(edge: EdgeTriplet[WikiVertex, Double]) =
      {

        if (edge.srcAttr.ns == 0 && edge.dstAttr.ns == 0) { // Article to Article
           if(edge.srcAttr.dist != Double.PositiveInfinity && edge.dstAttr.dist == Double.PositiveInfinity){
             Iterator((edge.dstId,edge.srcAttr.dist+1))
             
           }
           else{
             Iterator.empty
           }
        
        }
        else{
          Iterator.empty
        }
      }

    def messageCombiner(a: Double, b: Double): Double = {
      math.min(a,b)
    }
    // The initial message received by all vertices in PageRank

    val initialMessage = Double.PositiveInfinity

    val sssp = MyPregel(init,initialMessage, Int.MaxValue, EdgeDirection.Out)(
      vertexProgram,
      sendMessage,
      messageCombiner)

    println("----------------------------------------------")

    var summed = 1.0
   /* for(x <- sssp.vertices){
      println(x._2.title+ " "+x._2.dist)
    }*/
    val r=sssp.vertices.filter(x => if(x._2.ns==0  )true else false )
   
   val gq=sssp.vertices.filter(x => if(x._2.ns==0 && x._2.dist != Double.PositiveInfinity )true else false )
   println( "##**## Total number of articles -"+r.count)
   println("##*## Non zero distance articles -"+gq.count+ " for " + title)
   
    gq.saveAsTextFile("hdfs://dsg2.crc.nd.edu/data/enwiki/bfs_"+title)
    // var summed = sssp.vertices.map((a) => a._2.dist).reduce(math.max(_, _))

    (sssp, summed)
  }
    def main(args: Array[String]) {

  }
  
}