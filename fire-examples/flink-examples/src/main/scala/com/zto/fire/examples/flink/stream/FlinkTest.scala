package com.zto.fire.examples.flink.stream

import com.zto.fire._
import com.alibaba.fastjson.JSON
import com.zto.fire.examples.bean.Student
import com.zto.fire.flink.BaseFlinkStreaming
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

object FlinkTest extends BaseFlinkStreaming {

  /**
   * 生命周期方法：具体的用户开发的业务逻辑代码
   * 注：此方法会被自动调用，不需要在main中手动调用
   */
  override def process: Unit = {
    val dstream = this.fire.createKafkaDirectStream().map(json => {
      JSON.parseObject(json, classOf[Student])
    }).setParallelism(2)

    dstream.createOrReplaceTempView("student")
    val table = this.fire.sqlQuery("select * from student")

    // table无法序列化，因此需在此处获取schema信息，传入到addSink中
    val tableSchema = table.getTableSchema

    // toRetractStream支持状态更新、删除操作，比例sql中含有group by 等聚合操作，后进来的记录会导致已有的聚合结果不正确
    // 使用toRetractStream后会将之前的旧的聚合结果重新发送一次，并且tuple中的flag标记为false，然后再发送一条正确的结果
    // 类似于structured streaming中自动维护结果表，并进行update操作
    this.tableEnv.toRetractStream[Row](table).map(t => t._2).addSink(row => {
      println("-------->" + row.rowToBean(tableSchema, classOf[Student]))
    })

    // 不指定job name，则默认当前类名
    // this.fire.start
    this.fire.start("Fire Test")
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}
