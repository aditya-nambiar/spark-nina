package edu.nd.nina.wiki
import org.apache.spark.graphx._
object random_walk {

   def compute(g: Graph[WikiVertex, Double], rvid: VertexId): Graph[WikiVertex, Double] = {

    println("starting vertex id " + rvid)

    val (tp1, tp2) = bfs(g, rvid)

    tp1
  }
  
  def bfs(g: Graph[WikiVertex, Double], src: VertexId): (Graph[WikiVertex, Double], Double) = {
  
    val outdeg = g.outDegrees
    
    var init = g.mapVertices((id, x) => if (id == src) { new WikiVertex(100, x.ns, x.title) } else { new WikiVertex(0, x.ns, x.title) })
    
    val gr =init.joinVertices(outdeg)((vid,vd,u) =>  new WikiVertex(vd.dist, vd.ns, vd.title,false,false,List.empty,List.empty,u) )
    val alpha = 0.15
   
    def vertexProgram(src: VertexId, oldDist: WikiVertex, recmsgs: Double): WikiVertex =
      {
       
        
        if (oldDist.ns == 0 && recmsgs != 0) { //Article

          return new WikiVertex((recmsgs+oldDist.dist)*alpha, oldDist.ns, oldDist.title,false,false,List.empty,List.empty,oldDist.outdeg)

        }
        else{
          return oldDist
        }
      }

    def sendMessage(edge: EdgeTriplet[WikiVertex, Double]) =
      {

        if (edge.srcAttr.ns == 0 && edge.dstAttr.ns == 0) { // Article to Article
           if(edge.srcAttr.dist != 0){
             
             val i:Double = (1 - alpha)* edge.srcAttr.dist/edge.srcAttr.outdeg
             
             Iterator((edge.dstId,i))
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
      a+b
    }
    // The initial message received by all vertices in PageRank

    val initialMessage = 0.0
    
    val sssp = Pregel(gr,initialMessage, 8, EdgeDirection.Out)(
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