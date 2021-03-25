package com.zto.fire.examples.flink

import com.alibaba.fastjson.JSON
import com.zto.fire._
import com.zto.fire.common.util.JSONUtils
import com.zto.fire.examples.bean.Student
import org.apache.flink.api.scala._
import com.zto.fire.flink.BaseFlinkStreaming

/**
 * Flink流式计算任务模板
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2021-01-18 17:24
 */
object Test extends BaseFlinkStreaming {

  override def process: Unit = {
    val dstream = this.fire.createKafkaDirectStream().filter(JSONUtils.checkJson _).map(json => JSON.parseObject(json, classOf[Student]))
    dstream.createOrReplaceTempView("test")
    this.fire.sql("select t.name from test t left join dim.baseorganize_addzero b on t.name=o.fullname group by name").print()
    this.fire.start
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}
