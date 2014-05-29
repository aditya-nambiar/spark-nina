package edu.nd.nina.wiki

import org.apache.spark.SparkContext
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import scala.util.Random
import scala.collection.mutable.ArrayBuffer

object GenerateWikiGraph {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: LoadWikipediaArticles <master> <file>")
      System.exit(1)
    }

    PropertyConfigurator.configure("./conf/log4j.properties")

    val sparkconf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.apache.spark.graphx.GraphKryoRegistrator")

    val sc = new SparkContext(args(0), "LoadWikipediaArticles", sparkconf)

    val ty = generategraph(18, 20, 1, sc)

    val rt = ty.vertices.collect
    val rt2 = ty.edges.collect
    println("Vertices name followed by namespace")
    for (x <- rt) {
      println(x._2.title + " " + x._2.ns)
    }
    println("Edges Sources and destitnation ids")
    for (x <- rt2) {

      println(x.srcId + " " + x.dstId)
    }

    println(rt2.length)

    sc.stop

  }

  def generategraph(num_art: Int, num_cat: Int, branch: Int, sc: SparkContext): Graph[WikiVertex, Double] = {

    val art: Graph[WikiVertex, Double] =
      GraphGenerators.rmatGraph(sc, num_art, num_art * 5).mapVertices((id, _) => new WikiVertex(0, 0, id.toString)).mapEdges(x => x.attr.toDouble)

    val cat: Graph[Double, Int] =
      GraphGenerators.rmatGraph(sc, num_art, num_art * 2).mapVertices((id, _) => id.toDouble)
    val q = art.edges.filter(a => a.srcId != a.dstId)
    val cattp = cat.vertices
    val cattp2 = cat.edges.filter(a => a.srcId != a.dstId)
    // println("-------------------------------------")
    //for ( r <- cattp2){println( r.srcId+ " "+ r.dstId)}
    //println("-------------------------------------")
    val try1: RDD[(VertexId, WikiVertex)] = cattp.map(a => ((a._1.toInt + (num_art * 10)).toLong, new WikiVertex(0, 14, ((a._1 + (num_art * 10)).toString))))
    val try2: RDD[Edge[Double]] = cattp2.map(a => Edge(a.srcId + (num_art * 10), a.dstId + (num_art * 10), a.attr.toDouble))
    val try3: RDD[(VertexId, WikiVertex)] = try1.union(art.vertices)
    var try4: RDD[Edge[Double]] = try2.union(q)

    val catCol = try1.collect
    for (x <- art.vertices.collect) {
      var temp = ArrayBuffer.empty[Long]
      var ran = new Random() //Are all these automatically deleted?
      for (y <- 1 to branch) {
        val y = ran.nextInt(catCol.length)
        val num = catCol(y)._1
        // var num = num_art*10 + ran.nextInt(num_cat) 

        temp += num

      }

      val try5: RDD[Edge[Double]] = sc.parallelize((temp.map(z => Edge(x._1, z, 1.0))), 1)
      val try6: RDD[Edge[Double]] = sc.parallelize((temp.map(z => Edge(z, x._1, 1.0))), 4)

      try4 = try4.union(try5)
      try4 = try4.union(try6)

    }

    val categ = Graph[WikiVertex, Double](try3, try4)

    println(categ.edges.partitions.length)
    println(categ.vertices.partitions.length)

    categ

  }
}