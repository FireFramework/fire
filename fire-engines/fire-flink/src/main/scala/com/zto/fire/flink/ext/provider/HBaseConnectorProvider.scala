package com.zto.fire.flink.ext.provider

import com.zto.fire._
import com.zto.fire.hbase.bean.HBaseBaseBean
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.Table
import org.apache.flink.types.Row

import scala.reflect.ClassTag

/**
 * 为上层扩展层提供HBaseConnector API
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2020-12-24 10:16
 */
trait HBaseConnectorProvider {

  /**
   * hbase批量sink操作，DataStream[T]中的T必须是HBaseBaseBean的子类
   *
   * @param tableName
   * hbase表名
   * @param batch
   * 每次sink最大的记录数
   * @param flushInterval
   * 多久flush一次（毫秒）
   * @param keyNum
   * 配置文件中的key后缀
   */
  def hbasePutDS[T <: HBaseBaseBean[T]: ClassTag](stream: DataStream[T],
                                        tableName: String,
                                        batch: Int = 100,
                                        flushInterval: Long = 3000,
                                        keyNum: Int = 1): DataStreamSink[_] = {
    stream.hbasePutDS(tableName, batch, flushInterval, keyNum)
  }

  /**
   * hbase批量sink操作，DataStream[T]中的T必须是HBaseBaseBean的子类
   *
   * @param tableName
   * hbase表名
   * @param batch
   * 每次sink最大的记录数
   * @param flushInterval
   * 多久flush一次（毫秒）
   * @param keyNum
   * 配置文件中的key后缀
   * @param fun
   * 将dstream中的数据映射为该sink组件所能处理的数据
   */
  def hbasePutDS2[T <: HBaseBaseBean[T] : ClassTag](stream: DataStream[T],
                                                    tableName: String,
                                                    batch: Int = 100,
                                                    flushInterval: Long = 3000,
                                                    keyNum: Int = 1)(fun: T => T): DataStreamSink[_] = {
    stream.hbasePutDS2[T](tableName, batch, flushInterval, keyNum)(fun)
  }

  /**
   * table的hbase批量sink操作，该api需用户定义row的取数规则，并映射到对应的HBaseBaseBean的子类中
   *
   * @param tableName
   *                     HBase表名
   * @param batch
   *                     每次sink最大的记录数
   * @param flushInterval
   *                     多久flush一次（毫秒）
   * @param keyNum
   *                     配置文件中的key后缀
   */
  def hbasePutTable[T <: HBaseBaseBean[T]: ClassTag](table: Table,
                                           tableName: String,
                                           batch: Int = 100,
                                           flushInterval: Long = 3000,
                                           keyNum: Int = 1): DataStreamSink[_] = {
    table.hbasePutTable[T](tableName, batch, flushInterval, keyNum)
  }

  /**
   * table的hbase批量sink操作，该api需用户定义row的取数规则，并映射到对应的HBaseBaseBean的子类中
   *
   * @param tableName
   *                     HBase表名
   * @param batch
   *                     每次sink最大的记录数
   * @param flushInterval
   *                     多久flush一次（毫秒）
   * @param keyNum
   *                     配置文件中的key后缀
   */
  def hbasePutTable2[T <: HBaseBaseBean[T]: ClassTag](table: Table,
                     tableName: String,
                     batch: Int = 100,
                     flushInterval: Long = 3000,
                     keyNum: Int = 1)(fun: Row => T): DataStreamSink[_] = {
    table.hbasePutTable2[T](tableName, batch, flushInterval, keyNum)(fun)
  }
}
