package edu.nd.nina.wiki
import org.apache.spark.graphx._
import edu.nd.nina.test.MyPregel
object random_walk {

  def compute(g: Graph[WikiVertex, Double], rvid: Array[(VertexId, String)]): Graph[WikiVertex, Double] = {

    //  println("starting vertex id " + rvid)

    val (tp1, tp2) = bfs(g, rvid)

    tp1
  }

  def bfs(g: Graph[WikiVertex, Double], src: Array[(VertexId, String)]): (Graph[WikiVertex, Double], Double) = {

    val outdeg = g.outDegrees

    val z1 = src(0)._1
    val z2 = src(1)._1
    val z3 = src(2)._1
    val z4 = src(3)._1
    val z5 = src(4)._1
    val z6 = src(5)._1
    val z7 = src(6)._1
    val z8 = src(7)._1
    val z9= src(8)._1
    val z10= src(9)._1
    val z11= src(10)._1
    val z12= src(11)._1
    val z13= src(12)._1
    val z14= src(13)._1
    val z15= src(14)._1
    val z16= src(15)._1
    val z17= src(16)._1
    val z18= src(17)._1
    val z19= src(18)._1
    /*val z20= src(19)._1
    val z21= src(20)._1
    val z22= src(21)._1
    val z23= src(22)._1
    val z24= src(23)._1
    val z25= src(24)._1
    println("here1")*/

    //var init = g.mapVertices((id, x) => if (id == src) { new WikiVertex(0, x.ns, x.title) } else { new WikiVertex(0, x.ns, x.title) })
    var init = g.mapVertices((id, x) =>
      if (id == z1) {
        var d = new Array[Double](8)
        d(0) = 100

        new WikiVertex(x.dist, x.ns, x.title, false, false, List.empty, List.empty, d, 0)
      } else if (id == z2) {
        var d = new Array[Double](8)
        d(1) = 100
        new WikiVertex(x.dist, x.ns, x.title, false, false, List.empty, List.empty, d, 0)
      } else if (id == z3) {
        var d = new Array[Double](8)
        d(2) = 100
        new WikiVertex(x.dist, x.ns, x.title, false, false, List.empty, List.empty, d, 0)
      } else if (id == z4) {
        var d = new Array[Double](8)
        d(3) = 100
        new WikiVertex(x.dist, x.ns, x.title, false, false, List.empty, List.empty, d, 0)
      } else if (id == z5) {
        var d = new Array[Double](8)
        d(4) = 100
        new WikiVertex(x.dist, x.ns, x.title, false, false, List.empty, List.empty, d, 0)
      } else if (id == z6) {
        var d = new Array[Double](8)
        d(5) = 100
        new WikiVertex(x.dist, x.ns, x.title, false, false, List.empty, List.empty, d, 0)
      } else if (id == z7) {
        var d = new Array[Double](8)
        d(6) = 100
        new WikiVertex(x.dist, x.ns, x.title, false, false, List.empty, List.empty, d, 0)
      } else if (id == z8) {
        var d = new Array[Double](8)
        d(7) = 100
        new WikiVertex(x.dist, x.ns, x.title, false, false, List.empty, List.empty, d, 0)
      }    else if(id==z9) {var d= new Array[Double](25)
        d(8)=100
        new WikiVertex(x.dist, x.ns, x.title,false,false,List.empty,List.empty,d,0)}
      
       else if(id==z10) {var d= new Array[Double](25)
        d(9)=100
        new WikiVertex(x.dist, x.ns, x.title,false,false,List.empty,List.empty,d,0)}
      
        else if(id==z11) {var d= new Array[Double](25)
        d(10)=100
        new WikiVertex(x.dist, x.ns, x.title,false,false,List.empty,List.empty,d,0)}
      
       else if(id==z12) {var d= new Array[Double](25)
        d(11)=100
        new WikiVertex(x.dist, x.ns, x.title,false,false,List.empty,List.empty,d,0)}
      
        else if(id==z13) {var d= new Array[Double](25)
        d(12)=100
        new WikiVertex(x.dist, x.ns, x.title,false,false,List.empty,List.empty,d,0)}
      
        else if(id==z14) {var d= new Array[Double](25)
        d(13)=100
        new WikiVertex(x.dist, x.ns, x.title,false,false,List.empty,List.empty,d,0)}
      
        else if(id==z15){var d= new Array[Double](25)
        d(14)=100
        new WikiVertex(x.dist, x.ns, x.title,false,false,List.empty,List.empty,d,0)}
      
       else if(id==z16) {var d= new Array[Double](25)
        d(15)=100
        new WikiVertex(x.dist, x.ns, x.title,false,false,List.empty,List.empty,d,0)}
      
       else if(id==z17) {var d= new Array[Double](25)
        d(16)=100
        new WikiVertex(x.dist, x.ns, x.title,false,false,List.empty,List.empty,d,0)}
      
        else if(id==z18) {var d= new Array[Double](25)
        d(17)=100
        new WikiVertex(x.dist, x.ns, x.title,false,false,List.empty,List.empty,d,0)}
      
          else if(id==z19) {var d= new Array[Double](25)
        d(18)=100
        new WikiVertex(x.dist, x.ns, x.title,false,false,List.empty,List.empty,d,0)}
      /*
       else if(id==z20) {var d= new Array[Double](25)
        d(19)=100
        new WikiVertex(x.dist, x.ns, x.title,false,false,List.empty,List.empty,d,0)}
      
       else if(id==z21) {var d= new Array[Double](25)
        d(20)=100
        new WikiVertex(x.dist, x.ns, x.title,false,false,List.empty,List.empty,d,0)}
      
        else if(id==z22) {var d= new Array[Double](25)
        d(21)=100
        new WikiVertex(x.dist, x.ns, x.title,false,false,List.empty,List.empty,d,0)}
     
        else if(id==z23) {var d= new Array[Double](25)
        d(22)=100
        new WikiVertex(x.dist, x.ns, x.title,false,false,List.empty,List.empty,d,0)}
      
        else if(id==z24) {var d= new Array[Double](25)
        d(23)=100
        new WikiVertex(x.dist, x.ns, x.title,false,false,List.empty,List.empty,d,0)}
       
         else if(id==z25)  {var d= new Array[Double](25)
        d(24)=100
        new WikiVertex(x.dist, x.ns, x.title,false,false,List.empty,List.empty,d,0)}*/ else { new WikiVertex(x.dist, x.ns, x.title, false, false, List.empty, List.empty, new Array[Double](8), 0) })

    println("here2")

    val gr = init.joinVertices(outdeg)((vid, vd, u) => new WikiVertex(0, vd.ns, vd.title, false, false, List.empty, List.empty, vd.arr_dist, u))
    val alpha = 0.15
    // var k=0

    def vertexProgram(src: VertexId, oldDist: WikiVertex, recmsgs: Array[Double]): WikiVertex =
      {

        if (oldDist.ns == 0) { //Article
          //  println("Vertex Program" + recmsgs.length+" "+oldDist.arr_dist.length)
          var a = new Array[Double](8)
          for (x <- 0 to 7) {

            a(x) = (oldDist.arr_dist(x) + recmsgs(x)) * alpha
            if (a(x) != 0)
              println(a(x))

          }

          return new WikiVertex(0, oldDist.ns, oldDist.title, false, false, List.empty, List.empty, a, oldDist.outdeg)

        } else {
          return oldDist
        }
      }

    def sendMessage(edge: EdgeTriplet[WikiVertex, Double]) =
      { //println("doing ")
        //println("----------------------- New Step for "+title+ "--------------------------")
        if (edge.srcAttr.ns == 0 && edge.dstAttr.ns == 0) { // Article to Article

          var a = new Array[Double](8)
          for (x <- 0 to 7) {
            a(x) = (1 - alpha) * edge.srcAttr.arr_dist(x) / edge.srcAttr.outdeg
          }
          // val i:Double = (1 - alpha)* edge.srcAttr.dist/edge.srcAttr.outdeg

          Iterator((edge.dstId, a))

        } else {
          Iterator.empty
        }
      }

    def messageCombiner(a: Array[Double], b: Array[Double]): Array[Double] = {
      var c = new Array[Double](8)
      for (x <- 0 to 7) {
        c(x) = a(x) + b(x)
      }
      c
    }
    // The initial message received by all vertices in PageRank

    val initialMessage = new Array[Double](8)

    val sssp = MyPregel(gr, initialMessage, 7, EdgeDirection.Out)(
      vertexProgram,
      sendMessage,
      messageCombiner)

    println("----------------------------------------------")

    var summed = 1.0
    val r = sssp.vertices.filter(x => (x._2.arr_dist(0) != 0 || x._2.arr_dist(1) != 0 || x._2.arr_dist(2) != 0 || x._2.arr_dist(3) != 0 || x._2.arr_dist(4) != 0 || x._2.arr_dist(5) != 0 || x._2.arr_dist(6) != 0 || x._2.arr_dist(7) != 0))
    println("##*## Non-Zero articles = " + r.count)
    r.saveAsTextFile("hdfs://dsg2.crc.nd.edu/data/enwiki/random_12_trynew_with7")

    // var summed = sssp.vertices.map((a) => a._2.dist).reduce(math.max(_, _))

    (sssp, summed)
  }
  def main(args: Array[String]) {

  }
}