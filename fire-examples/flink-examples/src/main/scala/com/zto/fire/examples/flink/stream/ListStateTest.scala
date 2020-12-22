package com.zto.fire.examples.flink.stream

import com.zto.fire._
import com.zto.fire.flink.BaseFlinkStreaming
import com.zto.fire.flink.ext.functions.FireMapFunction
import org.apache.flink.api.common.state.StateTtlConfig.UpdateType
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

/**
 * 状态维护
 * @author ChengLong 2020-4-14 15:20:08
 */
object ListStateTest extends BaseFlinkStreaming {

  override def process: Unit = {
    val dstream = this.ssc.fromCollection(Seq((1, 1), (1, 2), (1, 3), (2, 1), (2, 2), (2, 3)))
    dstream.keyBy(1).flatMap(new FireMapFunction[(Int, Int), (Int, Int)] {
      var valueState: ValueState[(Int, Int)] = _

      override def open(parameters: Configuration): Unit = {
        // 设置状态的TTL时间
        val stateConfig = StateTtlConfig
          .newBuilder(Time.seconds(3))
          .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
          .setUpdateType(UpdateType.OnCreateAndWrite)
          .build()
        // 状态的描述信息
        val valueStateDescriptor = new ValueStateDescriptor("avgKeyedState", TypeInformation.of(new TypeHint[(Int, Int)] {}))
        valueStateDescriptor.enableTimeToLive(stateConfig)
        // 获取状态
        this.valueState = this.getState(valueStateDescriptor)
      }

      override def flatMap(value: (Int, Int), out: Collector[(Int, Int)]): Unit = {
        Thread.sleep(50000)
        var current = this.valueState.value()
        if (current == null)  {
          current = (0, 0)
          this.valueState.update(current)
        }
        println("之前值：" + this.valueState.value())
        this.valueState.update(value._1, current._2 + value._2)
        println("当前值：" + this.valueState.value())
        out.collect(value)
      }
    }).setParallelism(1).print()

    this.ssc.startAwaitTermination()
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}
