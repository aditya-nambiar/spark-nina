package edu.nd.nina.wiki

import org.apache.spark.{ Logging, SparkContext }
import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.graphx.Graph
import scala.reflect.ClassTag
import scala.util.Sorting
import org.apache.spark.util.collection.{ BitSet, OpenHashSet, PrimitiveVector }
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD

object WikiGraphLoader extends Logging {

  /**
   * Loads a graph from an edge list formatted file where each line contains two integers: a source
   * id and a target id. Skips lines that begin with `#`.
   *
   * If desired the edges can be automatically oriented in the positive
   * direction (source Id < target Id) by setting `canonicalOrientation` to
   * true.
   *
   * @example Loads a file in the following format:
   * {{{
   * # Comment Line
   * # Source Id <\t> Target Id
   * 1   -5
   * 1    2
   * 2    7
   * 1    8
   * }}}
   *
   * @param sc SparkContext
   * @param path the path to the file (e.g., /home/data/file or hdfs://file)
   * @param canonicalOrientation whether to orient edges in the positive
   *        direction
   * @param minEdgePartitions the number of partitions for the edge RDD
   */
  def edgeListFiles(
    sc: SparkContext,
    catpath: String,
    artpath: String,
    pagepath: String,
    canonicalOrientation: Boolean = false,
    minVertexPartitions: Int = 1,
    minEdgePartitions: Int = 1): Graph[WikiVertex, Int] =
    {
      val pages: RDD[(VertexId, Page)] = loadPage[Page](sc, pagepath, minVertexPartitions).setName("pages")
      val catGraph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, catpath, canonicalOrientation, minEdgePartitions)
      
      catGraph.vertices.setName("catGraph")
      catGraph.edges.setName("catGraph")

      val catPageGraph: Graph[Page, Int] = Graph(pages, catGraph.edges)
      
      
      catPageGraph.edges.setName("catPageGraph")

      var catToArtEdges: ArrayBuffer[Edge[Int]] = ArrayBuffer.empty
      catPageGraph.triplets.foreach(x => if (x.srcAttr != null && x.dstAttr != null && x.srcAttr.namespace == 0 && x.dstAttr.namespace == 14) {
        catToArtEdges += Edge(x.dstId, x.srcId, 1)
      })

      val catToArt = sc.parallelize(catToArtEdges, minEdgePartitions).setName("catToArt")

     
      val wikigraph: Graph[Page, Int] = Graph(
          Graph(
              pages, catGraph.edges).vertices, 
              catGraph.edges.union(GraphLoader.edgeListFile(sc, artpath, canonicalOrientation, minEdgePartitions).edges).union(catToArt)
              ).subgraph(x => true, (vid, vd) => vd != null).partitionBy(PartitionStrategy.EdgePartition2D).cache()

      catGraph.unpersistVertices(false)
      catGraph.edges.unpersist(false)
              
      wikigraph.vertices.setName("wikigraph")
      wikigraph.edges.setName("wikigraph")
      
      val cleanwikigraph = wikigraph
      
      cleanwikigraph.vertices.setName("cleanWikiGraph")
      cleanwikigraph.edges.setName("cleanWikiGraph")
      
      storeOutgoingNbrsInVertex(cleanwikigraph).cache
    }

  def storeOutgoingNbrsInVertex(wikigraph: Graph[Page, Int]): Graph[WikiVertex, Int] = {
    
    val wikifiedWikiGraph = wikigraph.mapVertices((vid, vd) => vd.toWikiVertex)
    
    val nbrs = wikifiedWikiGraph.mapReduceTriplets[Array[VertexId]](
      mapFunc = et =>
        if (et.srcAttr.ns == 0) {
          Iterator((et.srcId, Array(et.dstId)))
        } else {
          Iterator.empty
        },
      reduceFunc = _ ++ _)

      
    
    val neighborfiedVertices = wikifiedWikiGraph.vertices.leftZipJoin(nbrs) { (vid, vdata, nbrsOpt) =>
      vdata.neighbours = nbrsOpt.getOrElse(Array.empty[VertexId])
      vdata
    }

    Graph(neighborfiedVertices, wikigraph.edges);
    
  }

  def loadPage[VD: ClassManifest](
    sc: SparkContext,
    path: String,
    minEdgePartitions: Int = 1): RDD[(VertexId, Page)] = {

    val vertices = sc.textFile(path).flatMap { line =>
      if (!line.isEmpty && line(0) != '#') {
        val lineArray = line.split("\\s+")
        val vertexId = lineArray(0).trim().toInt
        val tail = lineArray.drop(1)
        val vdata = vertexParser(vertexId, tail)

        Iterator((vertexId: VertexId, vdata))
      } else {
        println("returning empty")
        Iterator.empty
      }
    }

    vertices
  }

  def vertexParser(vid: VertexId, arLine: Array[String]): Page = {
    if (arLine.length == 9) {
      new Page(arLine(0).toInt, arLine(1), arLine(2).toLong, arLine(3).toInt, arLine(4).toInt, arLine(5).toDouble, arLine(6).toLong, arLine(7).toInt, arLine(8).toInt)
    } else {
      logError("Error: Page tuple not correct format" + arLine.toString())
      null
    }
  }
}
