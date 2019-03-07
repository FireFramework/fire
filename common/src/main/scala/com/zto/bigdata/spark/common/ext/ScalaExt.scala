package com.zto.bigdata.spark.common.ext

import scala.collection.{JavaConversions, Seq, mutable}

/**
  * Scala API扩展
  * Created by ChengLong on 2017-03-30.
  */
object ScalaExt {

  /**
    * 集合扩展
    * @param seq
    * @tparam T
    */
  implicit class CollectionExt[T](seq: Seq[T]) {
    /**
      * 将scala seq转为Java list
      * @return
      */
    def toJavaList:java.util.List[T] = {
      JavaConversions.seqAsJavaList(seq)
    }
  }

  implicit class JavaMapExt[K, V](map: java.util.Map[K, V]) {
    def toScalaMap: mutable.Map[K, V] = {
      JavaConversions.mapAsScalaMap(map)
    }
  }

  implicit class JavaSetExt[T](set: java.util.Set[T]) {
    def toScalaSet: Set[T] = {
      JavaConversions.asScalaSet(set).toSet
    }
  }

}
