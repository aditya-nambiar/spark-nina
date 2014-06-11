package edu.nd.nina.data.drivers

import scala.Int.int2long

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

import edu.nd.nina.wiki.ComputeCategoryDistance
import edu.nd.nina.wiki.WikiGraphLoaderSmartly
import edu.nd.nina.wiki.WikiVertex

object LoadWikiGraphSmartly {

  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf()
    //sparkconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //sparkconf.set("spark.kryo.registrator", "edu.nd.nina.data.drivers.WikiRegistrator")
      //.setMaster("local[4]")
      .setMaster("spark://dsg1.virtual.crc.nd.edu:7077")
      .set("spark.driver.host", "129.74.153.244")
      .set("spark.driver.port", "5000")
      .set("spark.executor.memory", "14g")
      .set("spark.storage.memoryFraction", "0.5")
      .setAppName("t")
    .setJars(Array("./target/spark-nina-0.0.1-SNAPSHOT.jar"))

    val sc = new SparkContext(sparkconf)

    val g: Graph[WikiVertex, Double] = WikiGraphLoaderSmartly.edgeListFiles(sc, "hdfs://dsg2.crc.nd.edu/data/enwiki/categorylinks.txt", "hdfs://dsg2.crc.nd.edu/data/enwiki/pagelinks.txt", "hdfs://dsg2.crc.nd.edu/data/enwiki/page.txt", false, 100, 100).cache

    val vid = 12

    //ComputeCategoryDistance.compute(g, vid)
   
  }

}