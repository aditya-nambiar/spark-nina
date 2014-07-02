package edu.nd.nina.data.drivers

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import edu.nd.nina.wiki.LoadWikipedia
import edu.nd.nina.wiki.WikiArticle
import edu.nd.nina.wiki.ComputeCategoryDistanceSmartly
import org.apache.spark.graphx.Graph
import edu.nd.nina.wiki.WikiVertex
import org.apache.spark.graphx._
import edu.nd.nina.wiki.random_walk
import edu.nd.nina.wiki.bfs_articles
import edu.nd.nina.wiki.TwoDimcomputecateg
import org.apache.spark.rdd.RDD

object LoadWikipediaArticles extends Logging {

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: LoadWikipediaArticles <articles> <categories> <art->cat edges>")
      System.exit(1)
    }

    PropertyConfigurator.configure("./conf/log4j.properties")
    
    val sparkconf = new SparkConf()
      //sparkconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //sparkconf.set("spark.kryo.registrator", "org.apache.spark.graphx.GraphKryoRegistrator")
      //.setMaster("local[4]")
      .setMaster("spark://dsg1.virtual.crc.nd.edu:7077")

      .set("spark.driver.host", "129.74.143.99")
      .set("spark.driver.port", "6001")
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
      (8980330L, "WALL-E"))
      
      val ty = Graph(vertices,edges)

/*
    val sparkconf = new SparkConf()
    sparkconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkconf.set("spark.kryo.registrator", "edu.nd.nina.data.drivers.WikiRegistrator")
      //.setMaster("spark://dsg1.virtual.crc.nd.edu:7077")
      .setMaster("local[2]")
      .setAppName("t")
    //.setJars(Array("./target/spark-nina-0.0.1-SNAPSHOT.jar"))

    val sc = new SparkContext(sparkconf)

    //val (ty, vid) = //GenerateWikiGraph.generategraph(args(0).toInt, args(1).toInt, args(2).toInt, sc)
    val ty = LoadWikipedia.loadWikipedia(sc, "./data/enwiki_sample.xml", 4)
    val vid = WikiArticle.titleHash("A")
    
     val starts = Array((WikiArticle.titleHash("A"), "A"),
     (WikiArticle.titleHash("B"), "B"),
      (WikiArticle.titleHash("C"), "C"),
     (WikiArticle.titleHash("D"), "D"))*/
  

    
  // random_walk.compute(ty,vid) //----------Random Walk
 //  bfs_articles.compute(ty,vid)// ----------------BFS
 
    val catToArtEdges = ty.triplets.flatMap[Edge[Double]](x =>
      if (x.srcAttr != null && x.dstAttr != null && x.dstAttr.ns == 14) {
        Iterator(Edge(x.dstId, x.srcId, 1))
      } else {
        Iterator.empty
      })

    val edgeunion = ty.edges.union(catToArtEdges).setName("Unioned Edges RDD").cache
    //     val eu = edgeunion.count//<-executes
    //     println("Unioned Edges1: " + eu) 

    val wikigraph: Graph[WikiVertex, Double] = Graph(ty.vertices, edgeunion)

    val rt = ty.vertices.count
    val rt2 = ty.edges.count


    val nbrs = storeIncomingNbrsInVertex(wikigraph)
    
    val rvid = 18L

    val bcstNbrMap = sc.broadcast(nbrs)

   // ComputeCategoryDistanceSmartly.compute(wikigraph, vid, bcstNbrMap)
    println("going to compute")
    TwoDimcomputecateg.compute(ty,starts,bcstNbrMap)


    sc.stop

  }

  def storeIncomingNbrsInVertex(wikigraph: Graph[WikiVertex, Double]): Map[VertexId, Set[VertexId]] = {

    val nbrs = wikigraph.mapReduceTriplets[Set[VertexId]](
      mapFunc = et =>
        if (et.srcAttr.ns == 0 && et.dstAttr.ns == 0) {
          Iterator((et.dstId, Set(et.srcId)))
        } else {
          Iterator.empty
        },
      reduceFunc = _ ++ _)
     println("nbrs done")
    nbrs.toArray.toMap

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




