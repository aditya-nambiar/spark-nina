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
  var global: Graph[Wikivertex, Double] = _

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

    GraphGenerators.logNormalGraph(sc, numVertices = 100).mapVertices( (id, _) => id.toDouble )
    
    val g = loadWikipedia(sc, args(1), 4)

    //g.vertices.saveAsTextFile("hdfs://dsg2.crc.nd.edu/data/enwiki/aditya_vertices")
    //g.edges.saveAsTextFile("hdfs://dsg2.crc.nd.edu/data/enwiki/aditya_edges")

    sc.stop

  }

  def loadWikipedia(sc: SparkContext, rawData: String, numParts: Int): Graph[Wikivertex, Double] = {
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
    // for( (x,y) <- rt ){println(y.ns)}
    //  val tp4=cleanG.edges.collect
    // for(x <- tp4){println(x.srcId+ " -> "+ x.dstId+ " Weight " + x.attr)  }
    //   val (rvid, rvd) = ApproxDiameter.pickRandomVertex[String,Double](cleanG)
    val rvid = cleanG.vertices.first._1
    val temp = cleanG.mapTriplets(x => if (x.srcAttr.ns == 0 && x.dstAttr.ns == 0) -1.0 else 1.0)

    val (tp1, tp2) = sssp(temp, rvid)

    temp
  }

  def extractLinkGraph(sc: SparkContext, rawData: String, numParts: Int): (RDD[(VertexId, Wikivertex)], RDD[Edge[Double]]) = {
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

    val vertices: RDD[(VertexId, Wikivertex)] = wikiRDD.map { art => (art.vertexID, art.toWikivertex) }

    val edges: RDD[Edge[Double]] = wikiRDD.flatMap { art => art.edges }

    (vertices, edges)
  }

  def sssp(g: Graph[Wikivertex, Double], src: VertexId): (Graph[Wikivertex, Double], Double) = {

    val initGraph = g.mapVertices((id, x) => if (id == src) {new Wikivertex(0,x.ns,x.title)  } else { new Wikivertex(Double.PositiveInfinity,x.ns,x.title) })
    val a = g
    val b = 6
    global = g
    def vertexProgram(src: VertexId, oldDist: Wikivertex, newDist: Double): Wikivertex =
      {
          val c = math.min(oldDist.dist, newDist)
          val r = new Wikivertex(c,oldDist.ns,oldDist.title)
          r

      }

    def sendMessage(edge: EdgeTriplet[Wikivertex, Double]) =
      {

        if (edge.srcAttr.dist != Double.PositiveInfinity) {
          if (edge.srcAttr.ns == 0 && edge.dstAttr.ns == 0) {
            if (edge.attr == -1) {

              val src_in = edge.srcId
              val dst_in = edge.dstId
              val source=edge.srcAttr
              val dest =edge.dstAttr
              val initGraph_in = global.mapVertices((id, y) => if (id == src_in) { new Wikivertex(0,y.ns,y.title) } else { new Wikivertex(Double.PositiveInfinity,y.ns,y.title) })

              def vertexProgram_in(src: VertexId, oldDist: Wikivertex, newDist: Double): Wikivertex = {
             
                val c = math.min(oldDist.dist, newDist)
                val r = new Wikivertex(c,oldDist.ns,oldDist.title)
                r

              }

              def sendMessage_in(edge_in: EdgeTriplet[Wikivertex, Double]) = {

                if (edge_in.srcAttr.dist + edge_in.attr < edge_in.dstAttr.dist &&
                  ((edge_in.srcAttr.ns == 14 && edge_in.dstAttr.ns == 14) || (edge_in.srcId == src_in && edge_in.dstAttr.ns == 14) || (edge_in.dstId == dst_in && edge_in.srcAttr.ns == 14))) {

                  Iterator((edge_in.dstId, edge_in.srcAttr.dist + edge_in.attr))

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

           

              println(source.title + " " + dest.title + " " + v.first._2.dist)
              edge.attr = v.first._2.dist
            }

            if (edge.srcAttr.dist + edge.attr < edge.dstAttr.dist) {

              Iterator((edge.dstId, edge.srcAttr.dist + edge.attr))

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
    for ((x, y) <- tp1) { println(y.title + " " + y.dist) }
    var summed = sssp.vertices.map((a) => a._2.dist).reduce(math.max(_, _))

    (sssp, summed)
  }

}

class Wikivertex(a: Double, b: Int, c :String){
  var dist: Double = a
  var ns: Int = b
  var title: String = c
  
}