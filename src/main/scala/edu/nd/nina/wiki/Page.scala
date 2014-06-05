package edu.nd.nina.wiki

class Page(_namespace: Int, _title: String, _restrictions: Int, _counter: Long, _is_redirect: Boolean, _is_new: Boolean, _random: Double, _touched: Long, _latest: Int, _len: Int) {

  val namespace: Int = _namespace
  val title: String = _title
  val restrictions: Int = _restrictions
  val counter: Long = _counter
  val is_redirect: Boolean = _is_redirect
  val is_new: Boolean = _is_new
  val random: Double = _random
  val touched: Long = _touched
  val latest: Int = _latest
  val len: Int = _len
  
  def toWikiVertex():WikiVertex ={
    new WikiVertex(0, namespace, title, Array.empty, false, false, List.empty);
  }

}