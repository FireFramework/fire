package com.zto.bigdata.spark.common.ext

import scala.collection.{JavaConversions, Seq, mutable}

/**
  * Scala API扩展
  * Created by ChengLong on 2017-03-30.
  */
object ScalaExt {

  /**
    * 集合扩展
    *
    * @param seq
    * @tparam T
    */
  implicit class CollectionExt[T](seq: Seq[T]) {
    /**
      * 将scala seq转为Java list
      *
      * @return
      */
    def toJavaList: java.util.List[T] = {
      JavaConversions.seqAsJavaList(seq)
    }
  }

  /**
    * scala mutable map 转 java map
    */
  implicit class ScalaMutableMapExt[K, V](map: scala.collection.mutable.Map[K, V]) {
    /**
      * 将scala的mutable map 转为 java map
      *
      * @return
      */
    def toJavaMap: java.util.Map[K, V] = {
      JavaConversions.mutableMapAsJavaMap(map)
    }
  }

  /**
    * scala immutable map 转 java map
    */
  implicit class ScalaImmutableMapExt[K, V](map: scala.collection.immutable.Map[K, V]) {
    /**
      * 将scala的mutable map 转为 java map
      *
      * @return
      */
    def toJavaMap: java.util.Map[K, V] = {
      JavaConversions.mapAsJavaMap(map)
    }
  }

  /**
    * java map 转 scala map
    */
  implicit class JavaMapExt[K, V](map: java.util.Map[K, V]) {
    def toScalaMap: mutable.Map[K, V] = {
      JavaConversions.mapAsScalaMap(map)
    }
  }

  /**
    * java set 转 scala set
    */
  implicit class JavaSetExt[T](set: java.util.Set[T]) {
    def toScalaSet: Set[T] = {
      JavaConversions.asScalaSet(set).toSet
    }
  }

}
