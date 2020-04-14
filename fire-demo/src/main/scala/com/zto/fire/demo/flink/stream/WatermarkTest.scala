package com.zto.fire.demo.flink.stream

import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.zto.fire.common.util.DateFormatUtils
import com.zto.fire.demo.bean.Student
import com.zto.fire.flink.core.BaseFlinkStreaming
import com.zto.fire.flink.core.ext.FlinkExt._
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 水位线的使用要求：
 * 1. 开启EventTime：flink.stream.time.characteristic = EventTime
 * 2. 不同的task中有多个水位线实例，本地测试为了尽快看到效果，要降低并行度
 * 3. 多个task中的水位线会取最早的
 * 4. 水位线触发条件：1）多个task中时间最早的水位线时间 >= window窗口end时间  2）窗口中有数据
 * 5. 水位线是为了解决乱序和延迟数据的问题
 * 6. 乱序数据超过水位线的三种处理方式：1. 丢弃（默认） 2. allowedLateness，相当于进一步宽容的时间 3. sideOutputLateData：将延迟数据收集起来，统一处理
 *
 * @author ChengLong 2020-4-13 15:58:38
 */
object WatermarkTest extends BaseFlinkStreaming {

  override def process: Unit = {
    // source端接入消息并解析
    val dstream = this.ssc.createDirectStream().filter(str => StringUtils.isNotBlank(str) && str.contains("}")).map(str => {
      val student = JSON.parseObject(str, classOf[Student])
      (student, DateFormatUtils.formatDateTime(student.getCreateTime).getTime)
    })

    // 分配计算水位线
    val watermarkDS = dstream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(Student, Long)]() {
      var currentMaxTimestamp = 0L
      // 最大允许的乱序时间是10s
      val maxOutOfOrderness = 10000L
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

      /**
       * 1. 先执行抽取eventtime字段
       */
      override def extractTimestamp(element: (Student, Long), previousElementTimestamp: Long): Long = {
        // 用于根据当前事件时间计算水位线的值
        this.currentMaxTimestamp = Math.max(element._2, this.currentMaxTimestamp)
        println("---> 抽取eventtime：" + element._2 + " 最新水位线值：" + this.currentMaxTimestamp)
        element._2
      }

      /**
       * 2.获取当前水位线
       */
      override def getCurrentWatermark: Watermark = {
        new Watermark(this.currentMaxTimestamp - this.maxOutOfOrderness)
      }
    }).setParallelism(1) // 并行度调整为1的好处是能尽快观察到水位线的效果，否则要等多个task满足条件，不易观察结果

    // 用于存放延期的数据
    val outputTag = new OutputTag[(Student, Long)]("later_data")

    val windowDStream = watermarkDS
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      // 最大允许延迟的数据3s，算上水位线允许最大的乱序时间10s，一共允许最大的延迟时间为13s
      .allowedLateness(Time.seconds(3))
      // 收集延期的数据
      .sideOutputLateData(outputTag)
      .apply(new WindowFunctionTest)

    windowDStream.print().setParallelism(1)
    // 获取由于延迟太久而被丢弃的数据
    windowDStream.getSideOutput[(Student, Long)](outputTag).print()

    this.ssc.startAwaitTermination()
  }

  /**
   * 泛型说明：
   * 1. IN: The type of the input value.
   * 2. OUT: The type of the output value.
   * 3. KEY: The type of the key.
   */
  class WindowFunctionTest extends WindowFunction[(Student, Long), (Student, Long), Student, TimeWindow] {
    override def apply(key: Student, window: TimeWindow, input: Iterable[(Student, Long)], out: Collector[(Student, Long)]): Unit = {
      println("-->" + JSON.toJSONString(key, SerializerFeature.NotWriteRootClassName))
      val sortedList = input.toList.sortBy(_._2)
      sortedList.foreach(t => {
        println("---> " + JSON.toJSONString(t._1, SerializerFeature.NotWriteRootClassName))
        out.collect(t)
      })
    }
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}
