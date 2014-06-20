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

object LoadWikipediaArticles extends Logging {

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: LoadWikipediaArticles <articles> <categories> <art->cat edges>")
      System.exit(1)
    }

    PropertyConfigurator.configure("./conf/log4j.properties")

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



  // random_walk.compute(ty,vid) //----------Random Walk
 //  bfs_articles.compute(ty,vid)// ----------------BFS
 
/*    val catToArtEdges = ty.triplets.flatMap[Edge[Double]](x =>
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

    ComputeCategoryDistanceSmartly.compute(wikigraph, vid, bcstNbrMap)
*/

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

    nbrs.toArray.toMap

  }
}

