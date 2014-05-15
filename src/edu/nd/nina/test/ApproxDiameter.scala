package edu.nd.nina.test

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
<<<<<<< HEAD
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

=======
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._
import scala.collection.mutable.ListBuffer
import scala.util.Random
>>>>>>> upstream/master

object ApproxDiameter {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: ApproxDiameter <master> <file>")
      System.exit(1)

    }


    val sc = new SparkContext(args(0), "ApproxDiameter")
    var g = GraphLoader.edgeListFile(sc, args(1))
    doubleFringeDiameterEstimation(g.mapVertices((v, d) => d.toDouble))
    sc.stop
  }


  private def doubleFringeDiameterEstimation(g: Graph[Double, Int]) = {
    var lowerBound = Int.MinValue
    var upperBound = Int.MaxValue
    var iterationNo = 1
    val maxIterationNo = 3
    val maxVerticesToPickFromFringe = 5
    while (lowerBound < upperBound && iterationNo <= maxIterationNo) {

      val (rvid, rvd) = pickRandomVertex[Double, Int](g)
      println("picked r: " + rvid)
      val (h, lengthOfTreeFromR) = BreadthFirstSearch.sssp(g, rvid)
      println("lengthOfTreeFromR: " + rvid + " is " + lengthOfTreeFromR)
      // Pick a random vertex from the leaf
      val (avid, avd) = pickRandomVertices[Double, Int](h, vd => vd == lengthOfTreeFromR.toDouble, 1)(0)
      println("picked a: " + avid)
      val (t, lengthOfTreeFromA) = BreadthFirstSearch.sssp(g, avid)
      lowerBound = scala.math.max(lowerBound, lengthOfTreeFromA)
      val halfWay = lengthOfTreeFromA / 2
      println("lowerBound: " + lowerBound + " halfway: " + halfWay)
      // pick a vertex that's in the middle
      val (uvid, uvd) = pickRandomVertices[Double, Int](t, vd => vd == halfWay.toDouble, 1)(0)
      println("picked u: " + uvid)
      val (k, lengthOfTreeFromU) = BreadthFirstSearch.sssp(t, uvid)

      println("lengthOfTreeFromC: " + lengthOfTreeFromU)
      val fringeOfU = pickRandomVertices[Double, Int](k, vd => vd == lengthOfTreeFromU, maxVerticesToPickFromFringe)
      println("fringeOfC: " + fringeOfU)
      var maxDistance = -1
      for (src <- fringeOfU) {
        println("src: " + src)
        val (tmpG, lengthOfTreeFromSrc) = BreadthFirstSearch.sssp(k, src._1)
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

    }

    // POSTPROCESSING
    println("final lower bound:" + lowerBound + " final upperBound: " + upperBound)


  }

  def pickRandomVertex[VD, VE](g: Graph[VD, VE]): (VertexId, VD) = {
    pickRandomVertices[VD, VE](g, v => true, 1)(0)
  }

  def pickRandomVertices[VD, VE](g: Graph[VD, VE], p: VD => Boolean, numVerticesToPick: Int): Seq[(VertexId, VD)] = {
    val numVerticesToPickFrom = g.vertices.filter {
      case (id, d) => p(d)
    }.count()
    val actualNumVerticesToPick = scala.math.min(numVerticesToPickFrom, numVerticesToPick).intValue()
    val probability = 50 * actualNumVerticesToPick / numVerticesToPickFrom
    var found = false
    var retVal: ListBuffer[(VertexId, VD)] = ListBuffer()
    while (!found) {
      val selectedVertices = g.vertices.flatMap { v =>
        if (p(v._2) && Random.nextDouble() < probability) { Some(v) }
        else { None }
      }
      var collectedSelectedVertices = ListBuffer[(VertexId, VD)]()
      collectedSelectedVertices.appendAll(selectedVertices.collect())
      println("collectedSelectedVertices: " + collectedSelectedVertices)
      if (collectedSelectedVertices.size >= actualNumVerticesToPick) {
        found = true
        for (i <- 1 to actualNumVerticesToPick) {
          val randomIndex = Random.nextInt(collectedSelectedVertices.size)
          val toadd = collectedSelectedVertices(randomIndex)
          retVal.append(toadd)
          collectedSelectedVertices.remove(randomIndex)
        }
      } else {
        println("COULD NOT PICK A VERTEX. TRYING AGAIN....")
      }
    }
    retVal
  }
}
