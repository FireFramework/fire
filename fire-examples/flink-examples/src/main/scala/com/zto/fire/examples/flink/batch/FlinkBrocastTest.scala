package com.zto.fire.examples.flink.batch

import com.zto.fire.flink.BaseFlinkBatch
import com.zto.fire.flink.ext.FlinkExt._
import com.zto.fire.flink.ext.functions.FireMapFunction
import org.apache.flink.api.scala._

/**
 * flink广播变量的使用
 *
 * @author ChengLong 2020年2月18日 13:53:06
 */
object FlinkBrocastTest extends BaseFlinkBatch {

  override def process: Unit = {
    val ds = this.env.fromElements(1, 2, 3, 4, 5)
    // flink中可以广播的数据必须是Dataset
    val brocastDS = this.env.parallelize(Seq("a", "b", "c", "d", "e"))

    ds.map(new FireMapFunction[Int, String] {
      // 获取广播变量中的值给当前成员变量（若不想在open方法中获取值，请使用lazy关键字）
      lazy val broadcastSet: List[String] = this.getBroadcastVariable[String]("brocastDS")

      override def map(value: Int): String = {
        this.broadcastSet(value - 1)
      }

      // 每次使用必须通过withBroadcastSet进行广播
    }).withBroadcastSet(brocastDS, "brocastDS").print()
  }

  def main(args: Array[String]): Unit = {
    this.init()
    this.stop
  }
}
