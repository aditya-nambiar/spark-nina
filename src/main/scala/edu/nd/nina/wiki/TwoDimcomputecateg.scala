package edu.nd.nina.wiki

import org.apache.spark.graphx.Graph
import org.apache.spark.graphx._
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.Logging
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.List
import scala.collection.immutable.Set
import edu.nd.nina.test.ApproxDiameter
import edu.nd.nina.test.MyPregel
import org.apache.spark.Accumulator
import org.apache.spark.broadcast.Broadcast

object TwoDimcomputecateg  extends Logging{


def compute(g: Graph[WikiVertex, Double],rvid: Array[(VertexId, String)],bcstNbrMap:Broadcast[Map[VertexId, Set[VertexId]]]): Graph[WikiVertex, Double] = {
    
    
    println("starting vertex id " + rvid)

    val (tp1, tp2) = sssp(g, rvid,bcstNbrMap)

    tp1
  }

  def sssp(g: Graph[WikiVertex, Double], src: Array[(VertexId, String)],bcstNbrMap:Broadcast[Map[VertexId, Set[VertexId]]]): (Graph[WikiVertex, Double], Double) = {

    //var init = g.mapVertices((id, x) => if (id == src) { new WikiVertex(0, x.ns, x.title) } else { new WikiVertex(Double.PositiveInfinity, x.ns, x.title) })

    val z1 = src(0)._1
    val z2 = src(1)._1
    val z3 = src(2)._1
    val z4 = src(3)._1
  
  /*  val z5 = src(4)._1
    val z6 = src(5)._1
    val z7 = src(6)._1
    val z8 = src(7)._1
    val z9= src(8)._1
    val z10= src(9)._1*/
    val num_starts =4
  println("reached here\n")
    var init = g.mapVertices((id, x) =>
      if (id == z1) {
        var d = new Array[Double](num_starts)
         for( i <- 0 to num_starts-1){d(i)=Double.PositiveInfinity}
        d(0) = 0
       println(d(2))
        new WikiVertex(x.dist, x.ns, x.title, false, false, List.empty, List.empty, d, 0)
      } else if (id == z2) {
        var d = new Array[Double](num_starts)
         for( i <- 0 to num_starts-1){d(i)=Double.PositiveInfinity}
        d(1) = 0
        new WikiVertex(x.dist, x.ns, x.title, false, false, List.empty, List.empty, d, 0)
      } else if (id == z3) {
        var d = new Array[Double](num_starts)
         for( i <- 0 to num_starts-1){d(i)=Double.PositiveInfinity}
        d(2) = 0
        new WikiVertex(x.dist, x.ns, x.title, false, false, List.empty, List.empty, d, 0)
      } else if (id == z4) {
        var d = new Array[Double](num_starts)
       for( i <- 0 to num_starts-1){d(i)=Double.PositiveInfinity}
        d(3) = 0
        new WikiVertex(x.dist, x.ns, x.title, false, false, List.empty, List.empty, d, 0)
      }
    /*  } else if (id == z5) {
        var d = new Array[Double](10)
         d.foreach( y => Double.PositiveInfinity)
        d(4) = 0
        new WikiVertex(x.dist, x.ns, x.title, false, false, List.empty, List.empty, d, 0)
      } else if (id == z6) {
        var d = new Array[Double](10)
         d.foreach( y => Double.PositiveInfinity)
        d(5) = 0
        new WikiVertex(x.dist, x.ns, x.title, false, false, List.empty, List.empty, d, 0)
      } else if (id == z7) {
        var d = new Array[Double](10)
         d.foreach( y => Double.PositiveInfinity)
        d(6) = 0
        new WikiVertex(x.dist, x.ns, x.title, false, false, List.empty, List.empty, d, 0)
      } else if (id == z8) {
        var d = new Array[Double](10)
         d.foreach( y => Double.PositiveInfinity)
        d(7) = 0
        new WikiVertex(x.dist, x.ns, x.title, false, false, List.empty, List.empty, d, 0)
      }    else if(id==z9) {var d= new Array[Double](10)
         d.foreach( y => Double.PositiveInfinity)
        d(8)=0
        new WikiVertex(x.dist, x.ns, x.title,false,false,List.empty,List.empty,d,0)}
      
       else if(id==z10) {var d= new Array[Double](10)
          d.foreach( y => Double.PositiveInfinity)
        d(9)=0
        new WikiVertex(x.dist, x.ns, x.title,false,false,List.empty,List.empty,d,0)}
        
        */
      
      else { 
        var d= new Array[Double](num_starts)
        for( i <- 0 to num_starts-1){d(i)=Double.PositiveInfinity}
        
        new WikiVertex(x.dist, x.ns, x.title, false, false, List.empty, List.empty, d,0 ) })

      
    
       def vertexProgram(src: VertexId, oldDist: WikiVertex, recmsgs: Array[List[(VertexId, Double, Int)]]): WikiVertex =
      {
         
     
        if (oldDist.ns == 0) { //Article
        //  println("Akhil " + recmsgs(0).size +" "+ recmsgs(1).size+ " "+recmsgs(2).size+ " "+recmsgs(3).size+" "+oldDist.title)
         
          for(z <- 0 to num_starts-1){  if(recmsgs(z).size !=0){ recmsgs(z).reduce((x, y) => if (x._2 < y._2) x else y)}}
        //  recmsgs.foreach{ z => println(z.size)}
           var j =0
          var temp_dist = oldDist.arr_dist
          var ini_flag = false;
          var flag = false
           recmsgs.foreach{ x => 
             if(x.size !=0 && x.head._2 !=Double.PositiveInfinity && oldDist.arr_dist(j)==Double.PositiveInfinity){
              oldDist.arr_dist(j)= x.head._2 + 1              
             }
             if(x.size != 0 && x.head._1 == -1L)
             {ini_flag =true}
              j = j+1 
            }
           if(recmsgs(0).size==0 && recmsgs(1).size==0 && recmsgs(2).size==0 && recmsgs(3).size==0){ini_flag = true}
            
         
           var aliveflag = false
           oldDist.arr_dist.foreach( y => if(y == Double.PositiveInfinity) aliveflag = true )
           if(aliveflag== false){oldDist.isDead= true 
             println(oldDist.title + " died")}           
           for( i <- 0 to num_starts-1){
             if(temp_dist(i) != oldDist.arr_dist(i) || ini_flag==true)
             {flag= true}
           }
          
          return new WikiVertex(Double.PositiveInfinity, oldDist.ns, oldDist.title, oldDist.isDead, false, List.empty, List.empty,oldDist.arr_dist,0, recmsgs,flag)

        } 
        else { //Category
        
           for(y <- 0 to num_starts-1){ recmsgs(y)=recmsgs(y).filter(x => if (x._2 != Double.PositiveInfinity && x._3 < 10 ) true else false)}
           for(y <- 0 to num_starts-1){  recmsgs(y)=recmsgs(y).map(x => (x._1, x._2+1, x._3+1))}
       
          return new WikiVertex(Double.PositiveInfinity, oldDist.ns, oldDist.title, false, false, List.empty, List.empty,new Array[Double](4),0, recmsgs,false)
        }

      }

    def sendMessage(edge: EdgeTriplet[WikiVertex, Double]): Iterator[(VertexId, Array[List[(VertexId, Double, Int)]] )] =
      {

        if (edge.srcAttr.ns == 0 && edge.dstAttr.ns == 14) { // Article to Category
          if (edge.srcAttr.isDead == true) {
            Iterator.empty
          } else {
            
            
  
            if (edge.srcAttr.arr_dist(0)==Double.PositiveInfinity && edge.srcAttr.arr_dist(1)==Double.PositiveInfinity && edge.srcAttr.arr_dist(2)==Double.PositiveInfinity && edge.srcAttr.arr_dist(3)==Double.PositiveInfinity)// && edge.srcAttr.arr_tup(4)==Double.PositiveInfinity && edge.srcAttr.arr_tup(5)==Double.PositiveInfinity && edge.srcAttr.arr_tup(6)==Double.PositiveInfinity) {
            {  Iterator.empty
            } 
            else {	
                   var flag = false
	               var array_temp = Array[List[(VertexId, Double, Int)]]()	             
	              edge.srcAttr.arr_dist.foreach{z => 
	                if( z!= Double.PositiveInfinity){flag=true}
	                array_temp = array_temp :+ List((edge.srcId,z,0))
	               } 
	              
	              if( flag == false ){
	                Iterator.empty
	              }
	              else{
	              Iterator((edge.dstId,array_temp  ))}
            }
          }

        } else if (edge.srcAttr.ns == 14 && edge.dstAttr.ns == 14) { // Category to Category
          
          if (edge.srcAttr.arr_tup(0).size ==0 && edge.srcAttr.arr_tup(1).size ==0&& edge.srcAttr.arr_tup(2).size ==0&& edge.srcAttr.arr_tup(3).size ==0) {
            Iterator.empty
          } else {
           
            Iterator((edge.dstId, edge.srcAttr.arr_tup))
          }

        } 
        else if (edge.srcAttr.ns == 14 && edge.dstAttr.ns == 0) 
	        { // Category to Article
	          if(edge.dstAttr.isDead == true){
	           // println("Sending to " + edge.dstAttr.title)
	            return Iterator.empty
	          }
	          
	          
	          var temp_msg_buf = List.empty[(VertexId, Double, Int)]         
	          var array_temp = Array[List[(VertexId, Double, Int)]]() 
	                 
              
            for(y <- 0 to num_starts-1){ edge.srcAttr.arr_tup(y).foreach{x =>
            if(bcstNbrMap.value(edge.dstId).contains(x._1)){
              //x.last_cat = edge.srcId
             if(x._2 != Double.PositiveInfinity)
              temp_msg_buf = x :: temp_msg_buf
            }
           
            }
            
            array_temp =array_temp :+ temp_msg_buf
            temp_msg_buf = List.empty[(VertexId, Double, Int)]         
            
            }
	           
	       var flag = false
	       array_temp.foreach( y => if(y.size!=0) flag = true)
	       if(flag==true){
	       
	         Iterator((edge.dstId, array_temp))
	       }
	       else{
	         Iterator.empty
	       }
 
	          
         }
        
        else { //Article to ARticel edge
          Iterator.empty
        }
      }

    def messageCombiner(a: Array[List[(VertexId, Double,Int)]], b: Array[List[(VertexId, Double, Int)]]): Array[List[(VertexId, Double,Int)]] = {
      var c = Array[List[(VertexId, Double,Int)]]()
      for( i <- 0 to num_starts-1){
       var p = a(i) ::: b(i)
       c= c :+ p
      }
      var d = Array[List[(VertexId, Double,Int)]]()
      
      for(u <- 0 to num_starts-1){
        var q = c(u).groupBy(x => x._1).map(x => x._2.reduce((a, b) => if (a._2 < b._2) a else b))
         
        c(u) = q.toList
        //.map(x => x._2.reduce((a, b) => if (a._2 < b._2) a else b))
        
      }
          
          
          
     // val e = d.foreach( y=> y.map(x => x._2.reduce((a, b) => if (a._2 < b._2) a else b)). )
     // val e = d.foreach( )
    
      c
    }
    // The initial message received by all vertices in PageRank

    
   // var temp_msg_buf = List.empty[(VertexId, Double, Int)]         
	var array_temp =Array[List[(VertexId, Double, Int)]]()
	var tp = (-1L,Double.PositiveInfinity,0)
	var tp1 = List(tp,tp,tp)
	
	for( i <- 0 to num_starts-1){
	array_temp =array_temp :+ tp1}
	
	
	
	
    val sssp = MyPregel(init, array_temp,100,EdgeDirection.Out)(
      vertexProgram,
      sendMessage,
      messageCombiner)

    println("----------------------------------------------")

    var summed = 1.0
    
    for( x <- sssp.vertices){
      if( x._2.ns ==0){
      println("--###---####----####")
      println(x._2.title)
      println("A: "+x._2.arr_dist(0))
      println("B: "+x._2.arr_dist(1))
      println("C: "+x._2.arr_dist(2))
      println("D: "+x._2.arr_dist(3))
      }
      
    }
    
    // var summed = sssp.vertices.map((a) => a._2.dist).reduce(math.max(_, _))

    (sssp, summed)
  }

  def main(args: Array[String]) {

  }
}



