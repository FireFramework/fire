package com.zto.fire.examples.flink.stream

import com.zto.fire._
import com.alibaba.fastjson.JSON
import com.zto.fire.examples.bean.Student
import com.zto.fire.flink.BaseFlinkStreaming
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * window相当于将源源不断的流按一定的规则切分成不同的段，然后为每段分别计算
 * 当程序挂掉重启后，window中的数据不会丢失，会接着之前的window继续计算
 *
 * @author ChengLong 2020-4-18 14:34:58
 */
object WindowTest extends BaseFlinkStreaming {

  override def process: Unit = {
    val dstream = this.fire.createKafkaDirectStream().map(t => JSON.parseObject(t, classOf[Student])).map(s => (s.getName, s.getAge))
    this.testTimeWindow(dstream)

    this.fire.start
  }

  /**
   * 如果是keyedStream，则窗口函数为countWindow
   */
  private def testCountWindow(dstream: DataStream[(String, Integer)]): Unit = {
    dstream.keyBy(0)
      // 第一个参数表示窗口大小，窗口的容量是2条记录，达到2条会满，作为一个单独的window实例
      // 第二个参数如果不指定，则表示为滚动窗口（没有重叠），如果指定则为滑动窗口（有重叠）
      // 以下表示每隔1条数据统计一次window数据，而这个window中包含2条记录
      .countWindow(2, 1)
      .sum(1).print()
  }

  /**
   * 如果是普通的Stream，则窗口函数为countWindowAll
   */
  def testCountWindowAll(dstream: DataStream[(String, Integer)]): Unit = {
    // 表示每2条计算一次，每次将计算好的两条记录结果打印
    dstream.countWindowAll(2).sum(1).print()
  }

  /**
   * 时间窗口
   */
  def testTimeWindow(dstream: DataStream[(String, Integer)]): Unit = {
    // 窗口的宽度为1s，每隔1s钟处理过去1s的数据，这1s的时间内窗口中的记录数可多可少
    dstream.timeWindowAll(Time.seconds(1)).sum(1).print()
  }


  def main(args: Array[String]): Unit = {
    this.init()
  }
}
