package edu.nd.nina.wiki

class Page(_namespace: Int, _title: String, _counter: Long, _is_redirect: Int, _is_new: Int, _random: Double, _touched: Long, _latest: Int, _len: Int) extends java.io.Serializable {

  val namespace: Int = _namespace
  val title: String = _title
  val counter: Long = _counter
  val is_redirect: Int = _is_redirect
  val is_new: Int = _is_new
  val random: Double = _random
  val touched: Long = _touched
  val latest: Int = _latest
  val len: Int = _len
  
  def toWikiVertex():WikiVertex ={
    new WikiVertex(0, namespace, title, false, false, List.empty, List.empty);
  }

}