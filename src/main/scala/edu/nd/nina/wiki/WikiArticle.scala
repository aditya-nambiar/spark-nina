package edu.nd.nina.wiki

import java.util.regex.Pattern
import java.util.regex.Matcher
import scala.collection.mutable
import scala.xml._
import scala.collection.immutable.HashSet
import org.apache.spark.graphx._
import scala.Array.canBuildFrom

class WikiArticle(wtext: String) {
  val nsXML = WikiArticle.namespacePattern.findFirstIn(wtext).getOrElse("")
  val namespace: String = {
    try {
      XML.loadString(nsXML).text
    } catch {
      case e => WikiArticle.notFoundString // don't use null because we get null pointer exceptions
    }
  }
  val (links, extra): (Array[String], Array[String]) = WikiArticle.parseLinks(wtext, namespace)
  val neighbors = links.map(WikiArticle.titleHash).distinct
  val ee1 = extra.map(WikiArticle.titleHash).distinct
  // val namespace = WikiArticle.namespacePattern.findFirstIn(wtext).getOrElse("") 

  val redirect: Boolean = !WikiArticle.redirectPattern.findFirstIn(wtext).isEmpty
  val stub: Boolean = !WikiArticle.stubPattern.findFirstIn(wtext).isEmpty
  val disambig: Boolean = !WikiArticle.disambigPattern.findFirstIn(wtext).isEmpty
  val tiXML = WikiArticle.titlePattern.findFirstIn(wtext).getOrElse("")
  val title: String = {
    try {
      XML.loadString(tiXML).text
    } catch {
      case e => WikiArticle.notFoundString // don't use null because we get null pointer exceptions
    }
  }

  val newTitle = namespace match {
    case "0" => title
    case "14" => "Category:" + title
    case _ => null
  }

  val relevant: Boolean = !(redirect || stub || disambig || title == WikiArticle.notFoundString || title == null)
  val vertexID: VertexId = WikiArticle.titleHash(newTitle)
  val edges: HashSet[Edge[Double]] = {
    val temp = neighbors.map { n => Edge(vertexID, n, 1.0) }
    val ee2 = ee1.map { n => Edge(n, vertexID, 1.0) }
    val set = new HashSet[Edge[Double]]() ++ temp
    val set1 = set ++ ee2
    set1
  }

  def toWikivertex(): WikiVertex = {
    val a = new WikiVertex(0, namespace.toInt, title)
    a
  }
}
// val edges: HashSet[(VertexId, VertexId)] = {
//   val temp = neighbors.map { n => (vertexID, n) }
//   val set = new HashSet[(VertexId, VertexId)]() ++ temp
//   set
// }

object WikiArticle {
  val titlePattern = "<title>(.*)<\\/title>".r
  val namespacePattern = "<ns>(.*)<\\/ns>".r
  val redirectPattern = "#REDIRECT\\s+\\[\\[(.*?)\\]\\]".r
  val disambigPattern = "\\{\\{disambig\\}\\}".r
  val stubPattern = "\\-stub\\}\\}".r
  val linkPattern = Pattern.compile("\\[\\[(.*?)\\]\\]", Pattern.MULTILINE)

  val notFoundString = "NOTFOUND"

  private def parseLinks(wt: String, ns: String): (Array[String], Array[String]) = {
    val linkBuilder = new mutable.ArrayBuffer[String]()
    val ee = new mutable.ArrayBuffer[String]()
    val matcher: Matcher = linkPattern.matcher(wt)
    while (matcher.find()) {
      val temp: Array[String] = matcher.group(1).split("\\|")
      if (temp != null && temp.length > 0) {
        val link: String = temp(0)
        if (!link.contains(":") || link.startsWith("Category:")) {
          linkBuilder += link

          if (link.startsWith("Category:") && ns == "0") {
            {
              ee += link

            }
          }
        }
      }
    }
    return (linkBuilder.toArray, ee.toArray)
  }

  // substitute underscores for spaces and make lowercase
  private def canonicalize(title: String): String = {
    title.trim.toLowerCase.replace(" ", "_")
  }

  // Hash of the canonical article name. Used for vertex ID.
  // TODO this should be a 64bit hash
  private def titleHash(title: String): VertexId = { math.abs(WikiArticle.myHashcode(canonicalize(title))) }

  private def myHashcode(s: String): Long = {
    var h: Long = 1125899906842597L // prime
    // var h = 29
    val len: Int = s.length
    var i = 0
    while (i < len) {
      h = 31 * h + s.charAt(i)
      i += 1
    }
    h
    //     val md: MessageDigest = MessageDigest.getInstance("MD5")
    //     md.update(s.getBytes)
    //     val result: Array[Byte] = md.digest()
    //     val longResult = ByteBuffer.wrap(result).getLong
    //     // shift result by 2
    //     val retval = longResult >> 10
    //     retval
  }

}
