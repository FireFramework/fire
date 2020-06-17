package com.zto.fire.demo.flink.stream

import com.alibaba.fastjson.JSON
import com.zto.fire.common.db.HBaseOper
import com.zto.fire.common.util.PropUtils
import com.zto.fire.demo.bean.Student
import com.zto.fire.flink.core.BaseFlinkStreaming
import com.zto.fire.flink.core.ext.FlinkExt._
import org.apache.flink.api.scala._
import org.apache.flink.types.Row
import org.slf4j.LoggerFactory

object FlinkTest extends BaseFlinkStreaming {
  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * 生命周期方法：具体的用户开发的业务逻辑代码
   * 注：此方法会被自动调用，不需要在main中手动调用
   */
  override def process: Unit = {
    val maxParallelism = this.env.getMaxParallelism
    println("最大并行度：" + maxParallelism + " 默认并行度：" + this.env.getParallelism)
    this.env.parallelize(1 to maxParallelism).map(t => {
      PropUtils.compatible("flink")
    }).setParallelism(maxParallelism).print()
    HBaseOper.scan("test", HBaseOper.buildScan("0", "1"))
    val dstream = this.ssc.createDirectStream().map(json => {
      logger.error("FlinkTest {}", "task")
      logger.warn("FlinkTest {}", "task")
      logger.info("FlinkTest {}", "task")
      logger.debug("FlinkTest {}", "task")
      HBaseOper.scan("test", HBaseOper.buildScan("0", "1"))
      JSON.parseObject(json, classOf[Student])
    }).setParallelism(10)

    dstream.createOrReplaceTempView("student")
    val table = this.flink.sql("select * from student")

    // table无法序列化，因此需在此处获取schema信息，传入到addSink中
    val tableSchema = table.getTableSchema

    // toRetractStream支持状态更新、删除操作，比例sql中含有group by 等聚合操作，后进来的记录会导致已有的聚合结果不正确
    // 使用toRetractStream后会将之前的旧的聚合结果重新发送一次，并且tuple中的flag标记为false，然后再发送一条正确的结果
    // 类似于structured streaming中自动维护结果表，并进行update操作
    this.flink.toRetractStream[Row](table).map(t => t._2).addSink(row => {
      println("-------->" + row.rowToBean(tableSchema, classOf[Student]))
    })

    this.ssc.startAwaitTermination()
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}
