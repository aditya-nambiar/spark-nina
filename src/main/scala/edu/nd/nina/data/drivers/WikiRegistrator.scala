package edu.nd.nina.data.drivers

import org.apache.spark.graphx.GraphKryoRegistrator

import com.esotericsoftware.kryo.Kryo

import edu.nd.nina.wiki.WikiVertex

class WikiRegistrator extends GraphKryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    super.registerClasses(kryo)
    kryo.register(classOf[edu.nd.nina.wiki.WikiVertex])
  }
}
