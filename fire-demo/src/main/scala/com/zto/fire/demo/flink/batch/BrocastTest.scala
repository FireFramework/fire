package com.zto.fire.demo.flink.batch

import java.lang

import com.zto.fire.flink.core.BaseFlinkBatch
import org.apache.flink.api.common.functions.{RichMapFunction, RichMapPartitionFunction}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import com.zto.fire.flink.core.ext.FlinkExt._
import org.apache.flink.util.Collector

import scala.collection.{JavaConversions, mutable}

/**
 * flink广播变量的使用
 *
 * @author ChengLong 2020年2月18日 13:53:06
 */
object BrocastTest extends BaseFlinkBatch {

  override def process: Unit = {
    val ds = this.env.fromElements(1, 2, 3, 4, 5)
    // flink中可以广播的数据必须是Dataset
    val brocastDS = this.env.parallelize(Seq("a", "b", "c", "d", "e"))

    ds.map(new RichMapFunction[Int, String] {
      var broadcastSet: java.util.List[String] = null

      override def open(parameters: Configuration): Unit = {
        // 获取广播变量中的值给当前成员变量
        broadcastSet = this.getRuntimeContext.getBroadcastVariable[String]("brocastDS")
      }

      override def map(value: Int): String = {
        this.broadcastSet.get(value - 1)
      }
      // 每次使用必须通过withBroadcastSet进行广播
    }).withBroadcastSet(brocastDS, "brocastDS").print()

    // 使用mapPartition
    ds.mapPartition(new RichMapPartitionFunction[Int, String] {
      var list: java.util.List[String] = _

      override def open(parameters: Configuration): Unit = {
        this.list = this.getRuntimeContext.getBroadcastVariable("list")
      }

      override def mapPartition(values: lang.Iterable[Int], out: Collector[String]): Unit = {
        JavaConversions.asScalaIterator(values.iterator()).foreach(index => println(index + " " + this.list.get(index - 1)))
        this.list
      }
    }).withBroadcastSet(brocastDS, "list").print()
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}
