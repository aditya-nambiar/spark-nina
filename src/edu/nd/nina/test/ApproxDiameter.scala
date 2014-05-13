package edu.nd.nina.test

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

object ApproxDiameter {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: ApproxDiameter <master> <file>")
      System.exit(1)
    }
    
    val sc = new SparkContext(args(0), "ApproxDiameter")
    var g = GraphLoader.edgeListFile(sc, args(1))
    //doubleFringeDiameterEstimation(vertexFileName, edgesFileName)
  }
  
  private def doubleFringeDiameterEstimation(g: Graph[Int, Int]) = {
    var lowerBound = Int.MinValue
    var upperBound = Int.MaxValue
    var iterationNo = 1
    val maxIterationNo = 3
    val maxVerticesToPickFromFringe = 5
    while (lowerBound < upperBound && iterationNo <= maxIterationNo) {
  /*    
      val r = g.pickRandomVertex()
      println("picked r: " + r)
      val (h, lengthOfTreeFromR) = BreadthFirstSearch.sssp(g, r)
      g = h
      println("lengthOfTreeFromR: " + r + " is " + lengthOfTreeFromR)
      // Pick a random vertex from the leaf
      val a = g.pickRandomVertices(v => v.data == lengthOfTreeFromR, 1)(0)
      println("picked a: " + a)
      val (t, lengthOfTreeFromA) = runBfsFromVertexAndCountTheLengthOfTree(g, a)
      g = t
      lowerBound = scala.math.max(lowerBound, lengthOfTreeFromA)
      val halfWay = lengthOfTreeFromA / 2
      println("lowerBound: " + lowerBound + " halfway: " + halfWay)
      // pick a vertex that's in the middle
      val u = g.pickRandomVertices(v => v.data == halfWay, 1)(0)
      println("picked u: " + u)
      val (k, lengthOfTreeFromU) = runBfsFromVertexAndCountTheLengthOfTree(g, u)
      g = k
      println("lengthOfTreeFromC: " + lengthOfTreeFromU)
      val fringeOfU = g.pickRandomVertices(v => v.data == lengthOfTreeFromU, maxVerticesToPickFromFringe)
      println("fringeOfC: " + fringeOfU)
      var maxDistance = -1
      for (src <- fringeOfU) {
        println("src: " + src)
        val (tmpG, lengthOfTreeFromSrc) = runBfsFromVertexAndCountTheLengthOfTree(g, src)
        if (lengthOfTreeFromSrc > maxDistance) {
          maxDistance = lengthOfTreeFromSrc
        }
      }
      println("maxDistance: " + maxDistance + " fringOfU.size: " + fringeOfU.size)
      val twiceEccentricityMinus1 = ((2 * lengthOfTreeFromU) - 1)
      println("twiceEccentricityMinus1: " + twiceEccentricityMinus1)
      if (fringeOfU.size > 1) {
        if (maxDistance == twiceEccentricityMinus1) {
          upperBound = twiceEccentricityMinus1
          println("found the EXACT diameter: " + upperBound)
          assert(lowerBound == upperBound)
        } else if (maxDistance < twiceEccentricityMinus1) {
          upperBound = twiceEccentricityMinus1 - 1
        } else if (maxDistance == (twiceEccentricityMinus1 + 1)) {
          upperBound = lengthOfTreeFromA
          println("found the EXACT diameter with multiple fringe nodes but maxDistance matching the " +
          	"lengthOfTreeFromA. diameter: " + upperBound)
          assert(lengthOfTreeFromA == (twiceEccentricityMinus1 + 1))
          assert(lowerBound == upperBound)
        } else {
          println("ERROR maxDistance: " + maxDistance + " CANNOT be strictly > than twiceEccentricity: "
            + (twiceEccentricityMinus1 + 1))
          sys.exit(-1)
        }
      } else {
        // When there is a single vertex in the fringe of C or B(u) == twiceEccentricityMinus1
        // we find the exact diameter, which is equal to the diameter of T_u=lengthOfTreeFromA 
        upperBound = lengthOfTreeFromA
        println("found the EXACT diameter with single fringe node. diameter: " + upperBound)
        assert(lowerBound == upperBound)
      }
      upperBound = scala.math.min(upperBound, maxDistance)
      println("finished iterationNo: " + iterationNo + " lowerBound: " + lowerBound + " upperBound: " + upperBound)
      iterationNo += 1
   
   * 
   */ }    
   
    // POSTPROCESSING
    println("final lower bound:" + lowerBound + " final upperBound: " + upperBound)
   
  }
}
