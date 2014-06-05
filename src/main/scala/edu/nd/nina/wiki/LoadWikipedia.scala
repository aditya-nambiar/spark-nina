package edu.nd.nina.wiki

import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import org.apache.hadoop.io.LongWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text

object LoadWikipedia extends Logging {
  def loadWikipedia(sc: SparkContext, rawData: String, numParts: Int): Graph[WikiVertex, Double] = {
    val (vertices, edges) = extractLinkGraph(sc, rawData, numParts)

    val g = Graph(vertices, edges)
    g
  }

  def extractLinkGraph(sc: SparkContext, rawData: String, numParts: Int): (RDD[(VertexId, WikiVertex)], RDD[Edge[Double]]) = {
    val conf = new Configuration
    conf.set("key.value.separator.in.input.line", " ")
    conf.set("xmlinput.start", "<page>")
    conf.set("xmlinput.end", "</page>")

    val xmlRDD = sc.newAPIHadoopFile(rawData, classOf[XmlInputFormat], classOf[LongWritable], classOf[Text], conf)
      .map(t => t._2.toString).coalesce(numParts, false)
    val allArtsRDD = xmlRDD.map {
      raw => new WikiArticle(raw)
    }

    val wikiRDD = allArtsRDD.filter {
      art => art.relevant
    }.cache().setName("wikiRDD")
    logWarning(s"wikiRDD counted. Found ${wikiRDD.count} relevant articles in ${wikiRDD.partitions.size} partitions")

    val vertices: RDD[(VertexId, WikiVertex)] = wikiRDD.map { art => (art.vertexID, art.toWikivertex) }
     val edges: RDD[Edge[Double]] = wikiRDD.flatMap { art => art.edges }
     
    val gtemp = Graph(vertices,edges)
    val nbrs = gtemp.mapReduceTriplets[Array[VertexId]](
      mapFunc = et =>   if(et.dstAttr.ns == 0){
       Iterator((et.srcId, Array(et.dstId)))
       }else{
         Iterator.empty
       } ,
      reduceFunc = _ ++ _)
     val art2 = gtemp.vertices.leftZipJoin(nbrs) { (vid, vdata, nbrsOpt) =>
      vdata.neighbours = nbrsOpt.getOrElse(Array.empty[VertexId])
      vdata
    }

   

    (art2, edges)
  }
}