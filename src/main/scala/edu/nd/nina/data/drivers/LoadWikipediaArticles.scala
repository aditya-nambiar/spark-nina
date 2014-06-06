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
import edu.nd.nina.wiki._
import edu.nd.nina.wiki.GenerateWikiGraph
import edu.nd.nina.wiki.ComputeCategoryDistance

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
    val t1= ty.edges.collect
    for(x <- t1){println(x.srcId+ " " +x.dstId+ " " +x.attr)}
    val rt = ty.vertices.collect
    val rt2 = ty.edges.collect
    println("Vertices name followed by namespace")

    println("Edges Sources and destitnation ids")

    println(rt2.length)

    val rvid = 18L
    val temp = ty.mapTriplets(x => if (x.srcAttr.ns == 0 && x.dstAttr.ns == 0) -1.0 else 1.0)

    //ComputeCategoryDistance.compute(temp, vid)

    sc.stop

  }

}

