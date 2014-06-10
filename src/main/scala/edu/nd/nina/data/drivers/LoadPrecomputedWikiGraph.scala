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

object LoadPrecomputedWikiGraph extends Logging {

  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf()
      //sparkconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //sparkconf.set("spark.kryo.registrator", "org.apache.spark.graphx.GraphKryoRegistrator")
      //.setMaster("local[4]")
      .setMaster("spark://dsg1.virtual.crc.nd.edu:7077")
      .set("spark.driver.host", "129.74.153.244")
      .set("spark.driver.port", "5000")
      .set("spark.executor.memory", "14g")
      .set("spark.storage.memoryFraction", "0.5")
      .setAppName("t")
    .setJars(Array("./target/spark-nina-0.0.1-SNAPSHOT.jar"))

    val sc = new SparkContext(sparkconf)
    

    val vertices: RDD[(VertexId, WikiVertex)] = loadPrecomputedVertices(sc, "hdfs://dsg2.crc.nd.edu/data/enwiki/wikiDeg100vertices/", 18).setName("Vertices").cache
    val edges: RDD[Edge[Double]] = loadPrecomputedEdges(sc, "hdfs://dsg2.crc.nd.edu/data/enwiki/wikiDeg100edges/", 100).setName("Vertices").cache

    val g: Graph[WikiVertex, Double] = Graph(vertices, edges).cache

    val vid = 12

    ComputeCategoryDistance.compute(g, vid)

  }
  


  def loadPrecomputedVertices(
    sc: SparkContext,
    path: String,
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
      new WikiVertex(arLine(0).toDouble, arLine(1).toInt, arLine(2).toString, toNeighbors(arLine(3)))
    } else if (arLine.length == 3) {
      new WikiVertex(arLine(0).toDouble, arLine(1).toInt, arLine(2).toString, List.empty)
    } else {
      logError("Error: WikiVertex tuple not correct format" + arLine.toString())
      null
    }
  }

  def toNeighbors(line: String): List[VertexId] = {
    if (line.isEmpty()) {
      List.empty
    } else {

      val lineArray = line.split(",")
      var list: List[VertexId] = List.empty
      for (x <- lineArray) {
        list = x.toLong :: list
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