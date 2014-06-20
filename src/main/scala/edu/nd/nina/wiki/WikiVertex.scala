package edu.nd.nina.wiki

import org.apache.spark.graphx._
import scala.collection.immutable.List


class WikiVertex(a: Double, b: Int, c: String, e: Boolean, f: Boolean, g: List[Msg], h: List[(VertexId,Double,Int)], j : Array[Double],i: Int) extends java.io.Serializable {
  def this(a1: Double, b1: Int, c1: String) = this(a1, b1, c1, false, false, List.empty, List.empty,Array[Double](),0)
  def this(a1: Double, b1: Int, c1: String,  e: Boolean) = this(a1, b1, c1, e, false, List.empty, List.empty,Array[Double](),0)
  def this(a1: Double, b1: Int, c1: String,  e: Boolean, f: Boolean) = this(a1, b1, c1, e, f, List.empty, List.empty,Array[Double](),0)


  val dist: Double = a
  val ns: Int = b
  val title: String = c
  val col_msg: List[Msg] = g
  val tuples: List[(VertexId, Double,Int)] = h  
  val isDead = e
  val start: Boolean = f
  var outdeg : Int = i
  var arr_dist:Array[Double] = j
  override def toString(): String = {
    var out:String  =   " -" + dist + " World_War_One :"+arr_dist(0) +"\n"+
      "Barack_Obama :"+arr_dist(1)+ "\n"   +               
    "Dragon_ball :"     +arr_dist(2)+ "\n"   +               
   "WALL-E :"         +arr_dist(3)+ "\n"   +                             
   "Road_to_perdition :"      +arr_dist(4)+ "\n"   +                         
  "Britney_spears :"                  +arr_dist(5)+ "\n"   +               
   "United_kingdom :"                   +arr_dist(6)+ "\n"   +               
   "United_states :"                   +arr_dist(7)+ "\n"   +               
  "-----------------------------------------------------"
    out
  }
 
  
  
}