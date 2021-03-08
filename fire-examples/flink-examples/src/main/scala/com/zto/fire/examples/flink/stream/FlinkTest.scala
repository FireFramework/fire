package com.zto.fire.examples.flink.stream

import com.zto.fire._
import com.alibaba.fastjson.JSON
import com.zto.fire.common.util.{JSONUtils, PropUtils}
import com.zto.fire.examples.bean.Student
import com.zto.fire.flink.BaseFlinkStreaming
import com.zto.fire.flink.util.FlinkUtils
import org.apache.flink.api.scala._
import org.apache.flink.configuration.GlobalConfiguration
import org.apache.flink.runtime.util.EnvironmentInformation
import org.apache.flink.types.Row

object FlinkTest extends BaseFlinkStreaming {

  /**
   * 生命周期方法：具体的用户开发的业务逻辑代码
   * 注：此方法会被自动调用，不需要在main中手动调用
   */
  override def process: Unit = {
    val dstream = this.fire.createKafkaDirectStream().filter(str => JSONUtils.isJson(str)).map(json => {
      JSON.parseObject(json, classOf[Student])
    }).setParallelism(2)

    dstream.createOrReplaceTempView("student")
    val table = this.fire.sqlQuery("select * from student")
    println("flink.hello========>" + PropUtils.getString("flink.hello", "not_found"))
    // toRetractStream支持状态更新、删除操作，比例sql中含有group by 等聚合操作，后进来的记录会导致已有的聚合结果不正确
    // 使用toRetractStream后会将之前的旧的聚合结果重新发送一次，并且tuple中的flag标记为false，然后再发送一条正确的结果
    // 类似于structured streaming中自动维护结果表，并进行update操作
    this.tableEnv.toRetractStream[Row](table).map(t => t._2).addSink(row => {
      println("flink.hello========>" + PropUtils.getString("flink.hello", "not_found"))
      println("是否为TaskManager========>" + FlinkUtils.isJobManager)
      println("运行模式========>" + FlinkUtils.runMode)
    })

    // 不指定job name，则默认当前类名
    // this.fire.start
    this.fire.start("Fire Test")
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}
