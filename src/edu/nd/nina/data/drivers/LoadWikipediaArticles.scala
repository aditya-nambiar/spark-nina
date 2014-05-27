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

object LoadWikipediaArticles extends Logging {
  var global: Graph[String, Double] = _

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: LoadWikipediaArticles <master> <file>")
      System.exit(1)
    }

    PropertyConfigurator.configure("./conf/log4j.properties")

    val sparkconf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.apache.spark.graphx.GraphKryoRegistrator")
      

    val sc = new SparkContext("mesos://dsg1.crc.nd.edu:5050", "LoadWikipediaArticles", sparkconf)

    GraphGenerators.logNormalGraph(sc, numVertices = 100).mapVertices( (id, _) => id.toDouble )
    
    val g = loadWikipedia(sc, args(1), 4)

    g.vertices.saveAsTextFile("hdfs://dsg2.crc.nd.edu/data/enwiki/aditya_vertices")
    g.edges.saveAsTextFile("hdfs://dsg2.crc.nd.edu/data/enwiki/aditya_edges")

    sc.stop

  }

  def loadWikipedia(sc: SparkContext, rawData: String, numParts: Int): Graph[String, Double] = {
    val (vertices, edges) = extractLinkGraph(sc, rawData, numParts)

    val g = Graph(vertices, edges)
    println("yo " + g.vertices.count)
    logWarning("Graph has %d vertex partitions, %d edge partitions".format(g.vertices.partitions.length, g.edges.partitions.length))
    logWarning(s"DIRTY graph has ${g.triplets.count()} EDGES, ${g.vertices.count()} VERTICES")

    // TODO: try reindexing
    val cleanG = g.subgraph(x => true,
      (vid, vd) => vd != null).partitionBy(PartitionStrategy.EdgePartition2D).cache()
    cleanG.vertices.setName("cleanG vertices")
    cleanG.edges.setName("cleanG edges")

    logWarning(s"ORIGINAL graph has ${cleanG.triplets.count()} EDGES, ${cleanG.vertices.count()} VERTICES")
    logWarning(s"CLEAN graph has ${cleanG.triplets.count()} EDGES, ${cleanG.vertices.count()} VERTICES")
    println("CLean Graph")
    // val rt= cleanG.vertices.collect
    // for( (x,y) <- rt ){println(y.takeRight(2))}
    //  val tp4=cleanG.edges.collect
    // for(x <- tp4){println(x.srcId+ " -> "+ x.dstId+ " Weight " + x.attr)  }
    //   val (rvid, rvd) = ApproxDiameter.pickRandomVertex[String,Double](cleanG)
    val rvid = cleanG.vertices.first._1
    val temp = cleanG.mapTriplets(x => if (x.srcAttr.takeRight(1) == "0" && x.dstAttr.takeRight(1) == "0") -1.0 else 1.0)

    val (tp1, tp2) = sssp(temp, rvid)

    temp
  }

  def extractLinkGraph(sc: SparkContext, rawData: String, numParts: Int): (RDD[(VertexId, String)], RDD[Edge[Double]]) = {
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

    val vertices: RDD[(VertexId, String)] = wikiRDD.map { art => (art.vertexID, art.toString) }

    val edges: RDD[Edge[Double]] = wikiRDD.flatMap { art => art.edges }

    (vertices, edges)
  }

  def sssp(g: Graph[String, Double], src: VertexId): (Graph[String, Double], Double) = {

    val initGraph = g.mapVertices((id, x) => if (id == src) { "0:" + x.takeRight(1) } else { Double.PositiveInfinity.toString + ":" + x.takeRight(1) })
    val a = g
    val b = 6
    global = g
    def vertexProgram(src: VertexId, dist: String, newDist: Double): String =
      {
        val a = dist.dropRight(2).toDouble
        val b = newDist
        val c = math.min(a, b)
        val r = c.toString + dist.takeRight(2)
        r

      }

    def sendMessage(edge: EdgeTriplet[String, Double]) =
      {

        if (edge.srcAttr.dropRight(2).toDouble != Double.PositiveInfinity) {
          if (edge.srcAttr.takeRight(2) == ":0" && edge.dstAttr.takeRight(2) == ":0") {
            if (edge.attr == -1) {

              val src_in = edge.srcId
              val dst_in = edge.dstId

              val initGraph_in = global.mapVertices((id, y) => if (id == src_in) { "0:" + y.takeRight(1) } else { Double.PositiveInfinity.toString + ":" + y.takeRight(1) })

              def vertexProgram_in(src: VertexId, dist: String, newDist: Double): String = {
                val a = dist.dropRight(2).toDouble
                val b = newDist
                val c = math.min(a, b)
                val r = c.toString + dist.takeRight(2)
                r

              }

              def sendMessage_in(edge_in: EdgeTriplet[String, Double]) = {

                if (edge_in.srcAttr.dropRight(2).toDouble + edge_in.attr < edge_in.dstAttr.dropRight(2).toDouble &&
                  ((edge_in.srcAttr.takeRight(2) == ":4" && edge_in.dstAttr.takeRight(2) == ":4") || (edge_in.srcId == src_in && edge_in.dstAttr.takeRight(2) == ":4") || (edge_in.dstId == dst_in && edge_in.srcAttr.takeRight(2) == ":4"))) {

                  Iterator((edge_in.dstId, edge_in.srcAttr.dropRight(2).toDouble + edge_in.attr))

                } else {
                  Iterator.empty
                }

              }
              def messageCombiner_in(a: Double, b: Double): Double = {
                math.min(a, b)
              }

              // The initial message received by all vertices in PageRank
              val initialMessage_in = Double.PositiveInfinity

              val sssp_in = initGraph_in.pregel(initialMessage_in)(
                vertexProgram_in,
                sendMessage_in,
                messageCombiner_in)
              //Better method to find attr given id??
              var x = 4.0

              val v = sssp_in.vertices.filter(c => c._1 == dst_in)

              if (src_in == 34902897112120604L) {
                println("stop")
              }

              println(src_in + " " + dst_in + " " + v.first._2)
              edge.attr = v.first._2.dropRight(2).toDouble
            }

            if (edge.srcAttr.dropRight(2).toDouble + edge.attr < edge.dstAttr.dropRight(2).toDouble) {

              Iterator((edge.dstId, edge.srcAttr.dropRight(2).toDouble + edge.attr))

            } else { Iterator.empty }
          } else {
            Iterator.empty
          }

        }else{
          Iterator.empty
        }
      }
    def messageCombiner(a: Double, b: Double): Double = {
      math.min(a, b)
    }

    // The initial message received by all vertices in PageRank
    val initialMessage = Double.PositiveInfinity

    val sssp = initGraph.pregel(initialMessage)(
      vertexProgram,
      sendMessage,
      messageCombiner)

    println("----------------------------------------------")
    val tp1 = sssp.vertices.collect
    for ((x, y) <- tp1) { println(x + " " + y) }
    var summed = sssp.vertices.map((a) => a._2.dropRight(2).toDouble).reduce(math.max(_, _))

    (sssp, summed)
  }

}