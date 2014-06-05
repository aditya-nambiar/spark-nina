package edu.nd.nina.data.drivers

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx._
import edu.nd.nina.wiki.WikiGraphLoader
import edu.nd.nina.wiki.Page
import edu.nd.nina.wiki.WikiVertex
import edu.nd.nina.wiki.ComputeCategoryDistance

object LoadWikiGraph {

  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf()
    sparkconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //sparkconf.set("spark.kryo.registrator", "edu.nd.nina.data.drivers.WikiRegistrator")
      //.setMaster("spark://dsg1.virtual.crc.nd.edu:7077")
      .setMaster("local[6]")
      .setAppName("t")
    //.setJars(Array("./target/spark-nina-0.0.1-SNAPSHOT.jar"))

    val sc = new SparkContext(sparkconf)

    val g: Graph[WikiVertex, Int] = WikiGraphLoader.edgeListFiles(sc, "hdfs://dsg2.crc.nd.edu/data/enwiki/categorylinks.txt", "hdfs://dsg2.crc.nd.edu/data/enwiki/pagelinks.txt", "hdfs://dsg2.crc.nd.edu/data/enwiki/page.txt", false, 100, 100)

    val vid = 12

    ComputeCategoryDistance.compute(g, vid)
   
  }

}