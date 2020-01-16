package com.zto.fire.demo.flink.stream

import com.alibaba.fastjson.JSON
import com.zto.fire.demo.bean.Student
import com.zto.fire.flink.core.BaseFlinkStreaming
import com.zto.fire.flink.core.bean.FlinkTableSchema
import com.zto.fire.flink.core.ext.FlinkExt._
import com.zto.fire.flink.core.util.FlinkUtils
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

object FlinkRetractStreamTest extends BaseFlinkStreaming {

  /**
   * 生命周期方法：具体的用户开发的业务逻辑代码
   * 注：此方法会被自动调用，不需要在main中手动调用
   */
  override def process: Unit = {
    val dstream = this.ssc.createDirectStream().map(json => JSON.parseObject(json, classOf[Student]))
    dstream.createOrReplaceTempView("student")
    val table = this.flink.sql("select name,count(age) from student group by name")

    // toRetractStream支持状态更新、删除操作，比例sql中含有group by 等聚合操作，后进来的记录会导致已有的聚合结果不正确
    // 使用toRetractStream后会将之前的旧的聚合结果重新发送一次，并且tuple中的flag标记为false，然后再发送一条正确的结果
    // 类似于structured streaming中自动维护结果表，并进行update操作
    this.flink.toRetractStream[Row](table).print()

    this.ssc.startAwaitTermination()
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}
