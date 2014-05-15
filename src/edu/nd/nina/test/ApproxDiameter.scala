package edu.nd.nina.test

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import java.util.Random
import scala.collection.mutable.ArrayBuffer

object gbl{
def myrand() : Float  = {
  var r = new scala.util.Random
  return  r.nextInt(10000) / 10001
}
def hash_value(): Int = {
  var  ret:Int = 0;
  while (myrand() < 0.5) {
    ret= ret +1
  }
  return ret;
}
}
class vdata() {
	  var bitmask1 = new ArrayBuffer[ArrayBuffer[Boolean]]
      val bitmask2 = new ArrayBuffer[ArrayBuffer[Boolean]]
       /** for exact counting (but needs large memory) */
      def create_bitmask(id: Int){
          val mask1 = new ArrayBuffer[Boolean](id+2)
	      mask1(id) = true
	      bitmask1 += mask1
	      val mask2 = new ArrayBuffer[Boolean](id+2)
	      mask2(id) = true
	      bitmask2 += mask2
      }
      def create_hashed_bitmask( id: Int) {
    for (i <- 1 until 10) { /** DUPULICATION_OF_BITMASKS = 10 */
      var  hash_val :Int  = gbl.hash_value();
      val mask1= new ArrayBuffer[Boolean](hash_val+2)
      mask1(hash_val) = true
      bitmask1 += mask1
      val mask2= new ArrayBuffer[Boolean](hash_val+2)
      mask2(id)=true
      bitmask2 += mask2
      

    }
  }
}


object ApproxDiameter {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: ApproxDiameter <master> <file>")
      System.exit(1)
    }    
    
    
    val ctx = new SparkContext(args(0), "PageRank",
      System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass).toSeq)
    val graph = GraphLoader.edgeListFile(ctx, args(1));

    val pagerankGraph: Graph[vdata, Int] = graph
      // Associate the degree with each vertex
      .mapVertices( (id, attr) => vdata )
    

  }
  
  def vertexProgram(id: VertexId, attr: (Double, Double)): (Double, Double) = {
      val (oldPR, lastDelta) = attr
      val newPR = oldPR + (1.0 - resetProb) * msgSum
      (newPR, newPR - oldPR)
    }
  
}


