package com.zto.fire.demo.flink.state

import com.alibaba.fastjson.JSON
import com.zto.fire.common.util.JSONUtils
import com.zto.fire.demo.bean.Student
import com.zto.fire.flink.core.BaseFlinkStreaming
import com.zto.fire.flink.core.ext.FlinkExt._
import com.zto.fire.flink.core.ext.functions.FireMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/**
 *
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-10-22 16:25
 */
object ValueStateTest extends BaseFlinkStreaming {

  override def process: Unit = {
    val stream = this.env.createDirectStream().filter(JSONUtils.checkJson(_)).map(json => JSON.parseObject(json, classOf[Student]))
    val stateStream = stream.map(t => (t.getName, t)).keyBy(0).map(new FireMapFunction[(String, Student), (String, Int)]() {
      var state: ValueState[Int] = _

      override def open(parameters: Configuration): Unit = {
        val stateDec = new ValueStateDescriptor("maxState", classOf[Int])
        this.state = this.getRuntimeContext.getState(stateDec)
      }

      override def map(value: (String, Student)): (String, Int) = {
        val maxValue = this.state.value()
        val currentAge = value._2.getAge
        val maxAge = if (maxValue > currentAge) maxValue else currentAge.toInt
        state.update(maxAge)
        (value._1, maxAge)
      }
    })

    stateStream.print()

    this.env.execute()
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}
