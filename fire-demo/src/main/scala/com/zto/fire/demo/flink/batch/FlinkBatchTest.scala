package com.zto.fire.demo.flink.batch

import com.zto.fire.flink.BaseFlinkBatch
import com.zto.fire.flink.ext.FlinkExt._
import org.apache.flink.api.scala._

object FlinkBatchTest extends BaseFlinkBatch {

  /**
   * 生命周期方法：具体的用户开发的业务逻辑代码
   * 注：此方法会被自动调用，不需要在main中手动调用
   */
  override def process: Unit = {
    this.sc.parallelize(Seq("hello world hello")).flatMap(_.split(" ")).map(t => (t, 1)).groupBy(0).sum(1).print()
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}
