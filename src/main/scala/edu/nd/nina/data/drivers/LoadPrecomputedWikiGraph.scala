

package edu.nd.nina.data.drivers

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx._
import org.apache.spark.graphx.Edge
import org.apache.spark.Logging
import edu.nd.nina.wiki.WikiGraphLoader
import edu.nd.nina.wiki.Page
import edu.nd.nina.wiki.WikiVertex
import edu.nd.nina.wiki.ComputeCategoryDistance
import org.apache.spark.rdd.RDD
import org.apache.spark.AccumulatorParam
import edu.nd.nina.wiki.ComputeCategoryDistanceSmartly
import edu.nd.nina.wiki.random_walk
import edu.nd.nina.wiki.random_walk
import edu.nd.nina.wiki.bfs_articles

object LoadPrecomputedWikiGraph extends Logging {

  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf()
      //sparkconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //sparkconf.set("spark.kryo.registrator", "org.apache.spark.graphx.GraphKryoRegistrator")
      //.setMaster("local[4]")
      .setMaster("spark://dsg1.virtual.crc.nd.edu:7077")

      .set("spark.driver.host", "129.74.143.99")
      .set("spark.driver.port", "5001")
      .set("spark.executor.memory", "15g")
      .set("spark.driver.memory", "2g")
      .set("spark.storage.memoryFraction", "0.8")
      .set("spark.locality.wait", "100000000")
      .setAppName("t")
      .setJars(Array("./target/spark-nina-0.0.1-SNAPSHOT.jar"))

    val sc = new SparkContext(sparkconf)

    val vertices: RDD[(VertexId, WikiVertex)] = loadPrecomputedVertices(sc, "hdfs://dsg2.crc.nd.edu/data/enwiki/wikiDeg100vertices/", 50).setName("Vertices")
    val edges: RDD[Edge[Double]] = loadPrecomputedEdges(sc, "hdfs://dsg2.crc.nd.edu/data/enwiki/wikiDeg100edges/", 100).setName("Edges")

    val g: Graph[WikiVertex, Double] = Graph(vertices, edges)

    val vid = 12

    val starts = Array((4764461L, "World_War_I"),
      (534366L, "Barack_Obama"),
      (13078660L, "Dragon_ball"),
      (8980330L, "WALL-E"),
      (1318302L, "Road_to_perdition"),
      (740353L, "Britney_spears"),
      (8711726L, "United_kingdom"),
      (951976L, "United_states"),
      (11121L, "Fox_News_Channel"),
      (827390L, "Paul_mccartney "),
      (1246010L, "North_korea"),
      (2289648L, "Windows_Vista"),
      (9729299L, "Jonas_Brothers"),
      (1018340L, "Columbia_university"),
      (4854L, "Bermuda_Triangle"),
      (2466773L, "The_Godfather"),
      (5119376L, "John_F._Kennedy"),
      (12731L, "George_Harrison"),
      (5949664L, "FaceBook"),
      (20796L, "Malcolm_X"))

    random_walk.compute(g, starts)

  }

  def loadNeighbors(
    sc: SparkContext,
    path: String,
    minPartitions: Int = 1): scala.collection.immutable.Map[VertexId, Set[VertexId]] = {

    val vertices = sc.textFile(path, minPartitions).flatMap { line =>
      if (!line.isEmpty && line(0) != '#') {
        val vertexId = line.substring(1, line.indexOf(",")).toLong
        val lineArray = line.substring(line.indexOf(",") + 1, line.size - 1).split("\\s+")

        val vdata = neighborParser(vertexId, lineArray)

        Iterator((vertexId: VertexId, vdata))
      } else {
        println("returning empty")
        Iterator.empty
      }
    }
    vertices.toArray.toMap

  }

  def neighborParser(vid: VertexId, arLine: Array[String]): Set[VertexId] = {
    if (arLine.length == 4) {
      toNeighbors(arLine(3))
    } else if (arLine.length == 3) {
      Set.empty
    } else {
      logError("Error: WikiVertex tuple not correct format" + arLine.toString())
      null
    }
  }

  def loadPrecomputedVertices(
    sc: SparkContext,
    path: String,
    src: VertexId,
    minPartitions: Int = 1): RDD[(VertexId, WikiVertex)] = {

    val vertices = sc.textFile(path, minPartitions).flatMap { line =>
      if (!line.isEmpty && line(0) != '#') {
        val vertexId = line.substring(1, line.indexOf(",")).toLong
        val lineArray = line.substring(line.indexOf(",") + 1, line.size - 1).split("\\s+")
        val vdata = vertexParser(vertexId, lineArray)

        Iterator((vertexId: VertexId, vdata))
      } else {
        println("returning empty")
        Iterator.empty
      }
    }
    vertices

  }

  def vertexParser(vid: VertexId, arLine: Array[String]): WikiVertex = {
    if (arLine.length == 4) {
      new WikiVertex(arLine(0).toDouble, arLine(1).toInt, arLine(2).toString)
    } else if (arLine.length == 3) {
      new WikiVertex(arLine(0).toDouble, arLine(1).toInt, arLine(2).toString)
    } else {
      logError("Error: WikiVertex tuple not correct format" + arLine.toString())
      null
    }
  }

  def toNeighbors(line: String): Set[VertexId] = {
    if (line.isEmpty()) {
      Set.empty
    } else {

      val lineArray = line.split(",")
      var list: Set[VertexId] = Set.empty
      for (x <- lineArray) {
        list = list + x.toLong
      }
      list

    }
  }

  def loadPrecomputedEdges(
    sc: SparkContext,
    path: String,
    minPartitions: Int = 1): RDD[Edge[Double]] = {

    val edges = sc.textFile(path, minPartitions).flatMap { line =>
      if (!line.isEmpty && line(0) != '#') {
        val triple = line.substring(5, line.length() - 1).split(",")
        Iterator(Edge(triple(0).toLong, triple(1).toLong, 1d))
      } else {
        println("returning empty")
        Iterator.empty
      }
    }
    edges
  }

}

