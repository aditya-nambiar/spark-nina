package edu.nd.nina.test

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import scala.collection.mutable.ArrayBuffer


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
      .mapVertices( (id, _) => new vdata() )
      
    pagerankGraph.vertices.mapValues((x, vdata) => vdata.createbitmask(x))
    
    
      
      
    

  }
  
 // def vertexProgram(id: VertexId, attr: (Double, Double)): (Double, Double) = {
    //  val (oldPR, lastDelta) = attr
      //val newPR = oldPR + (1.0 - resetProb) * msgSum
     // (newPR, newPR - oldPR)
  //  }

}

class vdata () {
      val bitmask1: ArrayBuffer[ArrayBuffer[Boolean]] = ArrayBuffer();
      val bitmask2: ArrayBuffer[ArrayBuffer[Boolean]] = ArrayBuffer();
 
      def createbitmask(vid: VertexId) {
        
        val mask1 = ArrayBuffer.fill( (vid+2L).toInt ){ false }
	    mask1(vid.toInt) = true
	    
	    bitmask1.push_back(mask1);
	    std::vector<bool> mask2(id + 2, 0);
	    mask2[id] = 1;
	    bitmask2.push_back(mask2);
      }
}


