package edu.nd.nina.data.drivers

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.Logging
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.spark.storage.StorageLevel

object LoadWikipediaArticles extends Logging {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: LoadWikipediaArticles <master> <file>")
      System.exit(1)
    }

    val sparkconf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.apache.spark.graphx.GraphKryoRegistrator")

    val sc = new SparkContext(args(0), "LoadWikipediaArticles", sparkconf)
    
    val g = loadWikipedia(sc, args(1), 4)
    
    g.vertices.saveAsTextFile("hdfs://dsg2.crc.nd.edu/data/enwiki/aditya_vertices")
    g.edges.saveAsTextFile("hdfs://dsg2.crc.nd.edu/data/enwiki/aditya_edges")
    
    sc.stop

  }

  def loadWikipedia(sc: SparkContext, rawData: String, numParts: Int): Graph[WikiArticle, Double] = {
    val (vertices, edges) = extractLinkGraph(sc, rawData, numParts)

    val g = Graph(vertices, edges)
    logWarning("Graph has %d vertex partitions, %d edge partitions".format(g.vertices.partitions.length, g.edges.partitions.length))
    logWarning(s"DIRTY graph has ${g.triplets.count()} EDGES, ${g.vertices.count()} VERTICES")

    // TODO: try reindexing
    val cleanG = g.subgraph(x => true,
      (vid, vd) => vd != null).partitionBy(PartitionStrategy.EdgePartition2D).cache()
    cleanG.vertices.setName("cleanG vertices")
    cleanG.edges.setName("cleanG edges")

    logWarning(s"ORIGINAL graph has ${cleanG.triplets.count()} EDGES, ${cleanG.vertices.count()} VERTICES")
    logWarning(s"CLEAN graph has ${cleanG.triplets.count()} EDGES, ${cleanG.vertices.count()} VERTICES")

    cleanG
  }

  def extractLinkGraph(sc: SparkContext, rawData: String, numParts: Int): (RDD[(VertexId, WikiArticle)], RDD[Edge[Double]]) = {
    val conf = new Configuration
    conf.set("key.value.separator.in.input.line", " ")
    conf.set("xmlinput.start", "<page>")
    conf.set("xmlinput.end", "</page>")

    val xmlRDD = sc.newAPIHadoopFile(rawData, classOf[XmlInputFormat], classOf[LongWritable], classOf[Text], conf)
      .map(t => t._2.toString).coalesce(numParts, false)
    val allArtsRDD = xmlRDD.map {
      raw => new WikiArticle(raw) }

    val wikiRDD = allArtsRDD.filter {
      art => art.relevant
    }.cache().setName("wikiRDD")
    logWarning(s"wikiRDD counted. Found ${wikiRDD.count} relevant articles in ${wikiRDD.partitions.size} partitions")

    val vertices: RDD[(VertexId, WikiArticle)] = wikiRDD.map { art => (art.vertexID, art) }
    val edges: RDD[Edge[Double]] = wikiRDD.flatMap { art => art.edges }
    (vertices, edges)
  }

}