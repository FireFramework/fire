package com.zto.fire.examples.flink.batch

import java.lang
import java.util.UUID

import com.zto.fire.flink.BaseFlinkBatch
import com.zto.fire.flink.ext.functions.FireMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import com.zto.fire.flink.ext.FlinkExt._
import org.apache.flink.api.scala._

import scala.collection.JavaConversions

/**
 * 用于演示FireMapFunction的使用，FireMapFunction比RichMapFunction功能更强大
 * 提供了多值计数器、常用API函数的便捷使用等，甚至同时支持：map、flatMap、mapPartition等操作
 *
 * @author ChengLong 2020-4-9 15:59:19
 */
object FireMapFunctionTest extends BaseFlinkBatch {

  override def process: Unit = {
    val dataset = this.sc.parallelize(1 to 10)

    println("========使用FireMapFunction进行Map算子操作========")
    dataset.map(new FireMapFunction[Int, String]() {
      override def map(value: Int): String = {
        // FireMapFunction内置了一系列的api函数，均可通过this.方式调用
        // 1. this.getCacheFile("test")
        // 2. this.getBroadcastVariable("")
        // 3. this.getTaskName
        // 4. this.addMultiCounter("IntCount", 2) // 计数器使用详见：FlinkAccTest.scala
        // 5. this.addMultiCounter("LongCount", 3L)
        value.toString
      }
    }).print()

    println("========使用FireMapFunction进行Map算子操作========")
    dataset.mapPartition(new FireMapFunction[Int, String]() {
      override def open(parameters: Configuration): Unit = {
        // 执行初始化操作，如创建数据库连接池，调用次数与并行度一致
      }

      override def mapPartition(values: lang.Iterable[Int], out: Collector[String]): Unit = {
        JavaConversions.asScalaIterator(values.iterator()).foreach(i => out.collect(i.toString))
      }

      override def close(): Unit = {
        // 执行清理操作，如释放数据库连接，关闭文件句柄，调用次数与并行度一致
      }

    }).print()

    println("========使用FireMapFunction进行FlatMap算子操作========")
    dataset.flatMap(new FireMapFunction[Int, String] {
      override def flatMap(value: Int, out: Collector[String]): Unit = {
        out.collect(value + " - " + UUID.randomUUID().toString)
      }
    }).print()

    this.stop
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}
