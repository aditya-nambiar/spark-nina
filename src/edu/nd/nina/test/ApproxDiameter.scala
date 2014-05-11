package edu.nd.nina.test

import org.apache.spark.SparkContext
import org.apache.spark.graphx._


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
      .mapVertices( (id, attr) => vdata() )
    

  }
  
  def vertexProgram(id: VertexId, attr: (Double, Double)): (Double, Double) = {
      val (oldPR, lastDelta) = attr
      val newPR = oldPR + (1.0 - resetProb) * msgSum
      (newPR, newPR - oldPR)
    }
  
}

class vdata () {
      val bitmask1: Vector[Vector[Boolean]] = Vector();
      val bitmask2: Vector[Vector[Boolean]] = Vector();
 
      def createbitmask(id: VertexId){
        
      }
}


