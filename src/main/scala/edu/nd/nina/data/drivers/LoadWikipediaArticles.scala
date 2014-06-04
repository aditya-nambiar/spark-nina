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
import org.apache.log4j.Logger
import org.apache.log4j.PropertyConfigurator
import edu.nd.nina.test.ApproxDiameter
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.util.GraphGenerators
import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import edu.nd.nina.wiki.WikiVertex
import edu.nd.nina.wiki.WikiArticle
import edu.nd.nina.wiki.GenerateWikiGraph
import edu.nd.nina.wiki.ComputeCategoryDistance

object LoadWikipediaArticles extends Logging {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: LoadWikipediaArticles <master> <file>")
      System.exit(1)
    }

    PropertyConfigurator.configure("./conf/log4j.properties")

    val sparkconf = new SparkConf()
    sparkconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkconf.set("spark.kryo.registrator", "edu.nd.nina.data.drivers.WikiRegistrator")
    sparkconf.setMaster("local[1]").setAppName("LoadWikipediaArticles")

    val sc = new SparkContext(sparkconf)

    val (ty,vid) = GenerateWikiGraph.generategraph(1800, 400, 1, sc)

    val rt = ty.vertices.collect
    val rt2 = ty.edges.collect
    println("Vertices name followed by namespace")
   
    println("Edges Sources and destitnation ids")

    println(rt2.length)

    val rvid = 18L
    val temp = ty.mapTriplets(x => if (x.srcAttr.ns == 0 && x.dstAttr.ns == 0) -1.0 else 1.0)

    ComputeCategoryDistance.compute(temp, vid)

    sc.stop

  }

}

