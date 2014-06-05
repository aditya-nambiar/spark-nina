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

    val (ty, vid) = generategraph(1800, 200, 3, sc)

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

  def generategraph(num_art: Int, num_cat: Int, branch: Int, sc: SparkContext): (Graph[WikiVertex, Double], VertexId) = {

    val art1: Graph[WikiVertex, Double] =
      GraphGenerators.rmatGraph(sc, num_art, num_art * 5).mapVertices((id, _) => new WikiVertex(0, 0, id.toString, Array.empty[VertexId])).mapEdges(x => x.attr.toDouble)

    val nbrs = art1.mapReduceTriplets[Array[VertexId]](
      mapFunc = et => 
        if(et.srcId > 1){
        Iterator((et.srcId, Array(et.dstId)))
        }else{
          Iterator.empty
        },
      reduceFunc = _ ++ _)

    val cat: Graph[Double, Int] =
      GraphGenerators.rmatGraph(sc, num_cat, num_cat * 2).mapVertices((id, _) => id.toDouble)

    val art2 = art1.vertices.leftZipJoin(nbrs) { (vid, vdata, nbrsOpt) =>
      vdata.neighbours = nbrsOpt.getOrElse(Array.empty[VertexId])
      vdata
    }

    val art = Graph(art2, art1.edges);

    val q = art.edges.filter(a => a.srcId != a.dstId)
    val cattp = cat.vertices
    val cattp2 = cat.edges.filter(a => a.srcId != a.dstId)

    val try1: RDD[(VertexId, WikiVertex)] = cattp.map(a => (-a._1, new WikiVertex(0, 14, (-a._1).toString, Array.empty[VertexId])))

    val try2: RDD[Edge[Double]] = cattp2.map(a => Edge(-a.srcId, -a.dstId, a.attr.toDouble))
    val try3: RDD[(VertexId, WikiVertex)] = try1.union(art.vertices)
    var try4: RDD[Edge[Double]] = try2.union(q)

    var ab = ArrayBuffer.empty[Edge[Double]]
    var bc = ArrayBuffer.empty[Edge[Double]]
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

      ab = ab ++ temp.map(z => Edge(x._1, z, 1.0))
      bc = bc ++ temp.map(z => Edge(z, x._1, 1.0))

    }

    try4 = try4.union(sc.parallelize(ab, 4))
    try4 = try4.union(sc.parallelize(bc, 4))

    val categ = Graph[WikiVertex, Double](try3, try4)

    (categ, art1.vertices.first._1)

  }
}