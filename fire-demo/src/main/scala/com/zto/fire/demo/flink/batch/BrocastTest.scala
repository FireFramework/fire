package com.zto.fire.demo.flink.batch

import com.zto.fire.flink.core.BaseFlinkBatch
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

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
    val brocastDS = this.env.fromElements("a", "b", "c", "d", "e")

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
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}
