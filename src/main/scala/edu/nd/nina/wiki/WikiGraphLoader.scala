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
import org.apache.spark.graphx.EdgeRDD

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
    minEdgePartitions: Int = 1): Graph[WikiVertex, Double] =
    {
      val pages: RDD[(VertexId, Page)] = loadPage[Page](sc, pagepath, minVertexPartitions).setName("Pages").cache
      val pc = pages.count//<-executes
      println("Pages: " + pc)
      
      val catEdges = loadEdges(sc, catpath).setName("Category Edges").cache
      //val cc = catEdges.count//<-executes
      //println("Category Edges" + cc)

      val catPageGraph: Graph[Page, Double] = Graph(pages, catEdges)


      val catToArtEdges = catPageGraph.triplets.flatMap[Edge[Double]](x => 
         if (x.srcAttr != null && x.dstAttr != null && x.srcAttr.namespace == 0 && x.dstAttr.namespace == 14){
        	 Iterator(Edge(x.dstId, x.srcId, 1))
         }else{
        	 Iterator.empty
         }
         )
         
      catPageGraph.unpersistVertices(false);
      catPageGraph.edges.unpersist(false);
       
    //  val ca = catToArtEdges.count//<-executes
    //  println("Category To Article Edges" + ca)

      val edges = loadEdges(sc, artpath).setName("Article Edges").cache
     // val ec = edges.count//<-executes
     // println("Artical Edges: " + ec)
      
      val pageGraph = Graph(pages, edges).cache
      
      val deg10 = pageGraph.inDegrees.filter(v => if(v._2>=100) true else false)
      val deg10graph = pageGraph.vertices.innerJoin(deg10)((vid, vd, other) => vd)
      val filteredPageGraph = Graph(deg10graph, edges).cache
                
      
      val cleanfilteredwikigraph = filteredPageGraph.subgraph(x => x.srcAttr != null && x.dstAttr != null, (vid, vd) => vd != null)
      
      //println(cleanfilteredwikigraph.edges.count)
      
      
      val edgeunion = catEdges.union(cleanfilteredwikigraph.edges).union(catToArtEdges).setName("Unioned Edges RDD").cache
 //     val eu = edgeunion.count//<-executes
 //     println("Unioned Edges1: " + eu) 
      
      edges.unpersist(false)
      catToArtEdges.unpersist(false)
      catEdges.unpersist(false)
            
      val wikigraph: Graph[Page, Double] = Graph(pages, edgeunion)
      
      wikigraph.vertices.setName("WikiGraph Vertices").cache
      wikigraph.edges.setName("WikiGraph Edges").cache     
      
   //   val wvc = wikigraph.vertices.count//<-executes
   //   val wec = wikigraph.edges.count//<-executes
    //  println("WikiGraph Vertices: " + wvc)
   //   println("WikiGraph Edges: " + wec)
      
      edgeunion.unpersist(false)
      pages.unpersist(false)
        
      
      val junk  = loadjunkcategories(sc)   
    
      val cleanwikigraph = wikigraph.joinVertices(junk)((vid, vd, u) => u)//.filter(f => if(f. == -1) false else true )
      		.subgraph(x => x.srcAttr != null && x.dstAttr != null, (vid, vd) => vd != null && vd.namespace != -1)
      
      cleanwikigraph.vertices.setName("Clean WikiGraph Vertices").cache
      cleanwikigraph.edges.setName("Clean WikiGraph Edges").cache
      //val cwvc = cleanwikigraph.vertices.count//<-executes
      //val cwec = cleanwikigraph.edges.count//<-executes
      //println("WikiGraph Vertices: " + cwvc)
      //println("WikiGraph Edges: " + cwec)
      
      wikigraph.unpersistVertices(false)
      wikigraph.edges.unpersist(false)
      


      val wg = storeOutgoingNbrsInVertex(cleanwikigraph)
      wg.vertices.count
      
      cleanwikigraph.unpersistVertices(false)
      cleanwikigraph.edges.unpersist(false)
      
      println("------------------------------------")
      
      wg.vertices.saveAsTextFile("hdfs://dsg2.crc.nd.edu/data/enwiki/wikiDeg100vertices")
      wg.edges.saveAsTextFile("hdfs://dsg2.crc.nd.edu/data/enwiki/wikiDeg100edges")
      
      wg.vertices.foreach(
          f => if(f._1 == 780754 || f._1 == 12) 
            println(f._2.neighbours.length) 
          )
      
      wg
    }

  def storeOutgoingNbrsInVertex(wikigraph: Graph[Page, Double]): Graph[WikiVertex, Double] = {

    val wikifiedWikiGraph = wikigraph.mapVertices((vid, vd) => vd.toWikiVertex)

    val nbrs = wikifiedWikiGraph.mapReduceTriplets[List[VertexId]](
      mapFunc = et =>
        if (et.srcAttr.ns == 0) {
          Iterator((et.srcId, List(et.dstId)))
        } else {
          Iterator.empty
        },
      reduceFunc = _ ++ _)

    val neighborfiedVertices = wikifiedWikiGraph.vertices.leftZipJoin(nbrs) { (vid, vdata, nbrsOpt) =>
      if(vid == 12){
        println(12)
      }
      vdata.neighbours = nbrsOpt.getOrElse(List.empty[VertexId])
      vdata
    }

    Graph(neighborfiedVertices, wikigraph.edges);

  }

  def loadjunkcategories(sc: SparkContext): RDD[(VertexId, Page)] = {

    val cats = sc.textFile("hdfs://dsg2.crc.nd.edu/data/enwiki/junkcategories").flatMap { line =>
      if (!line.isEmpty && line(0) != '#') {

        Iterator((line.toLong, new Page(-1, "", 0l, 0, 0, 0.0, 0l, 0, 0)))
      } else {
        println("returning empty")
        Iterator.empty
      }
    }

    cats

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

  def loadEdges[VD: ClassManifest](
    sc: SparkContext,
    path: String,
    minEdgePartitions: Int = 1): RDD[Edge[Double]] = {

    
    val edges = sc.textFile(path).flatMap { line =>      
      if (!line.isEmpty && line(0) != '#') {
        val lineArray = line.split("\\s+")
        val srcId = lineArray(0).trim().toInt
        val dstId = lineArray(1).trim().toInt

        Iterator(Edge(srcId, dstId, 1d))
      } else {
        println("returning empty")
        Iterator.empty
      }
    }

    edges
  }

  def vertexParser(vid: VertexId, arLine: Array[String]): Page = {
    if (arLine.length == 9) {
      new Page(arLine(0).toInt, arLine(1), arLine(2).toLong, arLine(3).toInt, arLine(4).toInt, arLine(5).toDouble, arLine(6).toLong, arLine(7).toInt, arLine(8).toInt)
    }else if(arLine.length == 10){
      new Page(arLine(0).toInt, arLine(1), arLine(3).toLong, arLine(4).toInt, arLine(5).toInt, arLine(6).toDouble, arLine(7).toLong, arLine(8).toInt, arLine(9).toInt)
    } else {
      logError("Error: Page tuple not correct format" + arLine.toString())
      null
    }
  }
}
