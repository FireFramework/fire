package com.zto.fire.demo.flink.batch

import com.zto.fire.flink.core.BaseFlinkBatch
import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem
import com.zto.fire.flink.core.ext.FlinkExt._

object FlinkBatchTest extends BaseFlinkBatch {

  /**
   * 生命周期方法：具体的用户开发的业务逻辑代码
   * 注：此方法会被自动调用，不需要在main中手动调用
   */
  override def process: Unit = {
    /*this.sc.parallelize(Seq("hello world hello")).flatMap(_.split(" ")).map(t => {
      (t, 1)
    }).groupBy(0).sum(1).print()*/
    this.testAccumulator
  }

  def testAccumulator: Unit = {
    val result = this.sc.parallelize(1 to 10).map(new RichMapFunction[Int, Int] {
      val counter = new IntCounter()

      override def open(parameters: Configuration): Unit = {
        this.getRuntimeContext.addAccumulator("myCounter", this.counter)
      }

      override def map(value: Int): Int = {
        this.counter.add(value)
        value
      }
    })
    result.writeAsText("J:\\test\\flink.result", FileSystem.WriteMode.OVERWRITE)

    val result2 = this.sc.parallelize(1 to 10).map(new RichMapFunction[Int, Int] {
      override def map(value: Int): Int = {
        this.getRuntimeContext.getIntCounter("myCounter").add(value)
        value
      }
    })
    result2.writeAsText("J:\\test\\flink.result", FileSystem.WriteMode.OVERWRITE)
    val count = this.env.execute("counter").getAccumulatorResult[Int]("myCounter")
    println("累加器结果：" + count)
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}
