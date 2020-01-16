package com.zto.fire.flink.core.ext.stream

import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector
import com.zto.fire.common.bean.HBaseBaseBean
import com.zto.fire.common.util.GlobalConstants
import com.zto.fire.core.bridge.HBaseSparkBridge
import com.zto.fire.flink.core.sink.{HBaseOperSink, HBaseOperSinkBatch}
import com.zto.fire.flink.core.util.FlinkSingletonFactory
import org.apache.flink.api.common.accumulators.SimpleAccumulator
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.util.Collector

import scala.reflect.{ClassTag, classTag}

/**
 * 用于对Flink DataStream的API库扩展
 *
 * @author ChengLong 2020年1月7日 09:18:21
 * @since 0.4.1
 */
class DataStreamExt[T](dataStream: DataStream[T]) {
  lazy val tableEnv = FlinkSingletonFactory.getStreamTableEnv

  /**
   * 将流注册为临时表
   *
   * @param tableName
   * 临时表的表名
   */
  def createOrReplaceTempView(tableName: String): Table = {
    val table = this.dataStream.toTable(this.tableEnv)
    this.tableEnv.registerTable(tableName, table)
    table
  }

  /**
   * 预先注册flink累加器
   *
   * @param acc
   * 累加器实例
   * @param name
   * 累加器名称
   * @return
   * 注册累加器之后的流
   */
  def registerAcc(acc: SimpleAccumulator[_], name: String): DataStream[String] = {
    this.dataStream.map(new RichMapFunction[T, String] {
      override def open(parameters: Configuration): Unit = {
        this.getRuntimeContext.addAccumulator(name, acc)
      }

      override def map(value: T): String = value.toString
    })
  }

  /**
   * 将流数据实时写入到hbase中
   *
   * @param tableName
   * hbase 表名
   * @param insertEmpty
   * 为空的字段是否插入
   * @param batchSize
   * 一个批次写入的数据量
   * @param multiVersion
   * 是否支持多版本插入
   * @author ChengLong 2020年1月15日 16:11:08
   * @since 0.4.1
   */
  def hbaseOperPut[E <: HBaseBaseBean[E] : ClassTag](tableName: String, insertEmpty: Boolean = true, batchSize: Int = GlobalConstants.HBaseConf.hbaseBatchSize, multiVersion: Boolean = false): Unit = {
    if (Math.abs(batchSize) == 1) {
      this.dataStream.asInstanceOf[DataStream[E]].addSink(new HBaseOperSink[E](tableName, insertEmpty, multiVersion))
    } else {
      this.countWindowSimple(batchSize).addSink(new HBaseOperSinkBatch[E](tableName, insertEmpty, multiVersion))
    }
  }

  /**
   * 将流映射为批流
   *
   * @param count
   * 将指定数量的合并为一个集合
   */
  def countWindowSimple[T: ClassTag](count: Long): DataStream[List[T]] = {
    implicit val typeInfo = TypeInformation.of(classOf[List[T]])
    dataStream.asInstanceOf[DataStream[T]].countWindowAll(Math.abs(count)).apply(new AllWindowFunction[T, List[T], GlobalWindow]() {
      override def apply(window: GlobalWindow, input: Iterable[T], out: Collector[List[T]]): Unit = {
        out.collect(input.toList)
      }
    })(typeInfo)
  }
}
