package com.zto.fire.flink.core.ext.stream

import com.zto.fire.common.util.ValueUtils
import com.zto.fire.flink.core.bean.FlinkTableSchema
import com.zto.fire.flink.core.util.FlinkSingletonFactory
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.types.Row

import scala.collection.mutable.ListBuffer


/**
 * 用于flink StreamTable API库扩展
 *
 * @author ChengLong 2020年1月9日 13:52:16
 * @since 0.4.1
 */
class TableExt(table: Table) {
  lazy val streamTableEnv = FlinkSingletonFactory.getStreamTableEnv
  lazy val batchTableEnv = FlinkSingletonFactory.getBatchTableEnv

  /**
   * 逐条打印每行记录
   */
  def show: Unit = {
    this.table.addSink(row => println(row))
  }

  /**
   * 获取表的schema包装类，用于flinkRowToBean
   *
   * @return
   * fire包装后的表schema信息
   */
  def getTableSchema: FlinkTableSchema = {
    new FlinkTableSchema(table.getSchema)
  }

  /**
   * 将Table转为追加流
   */
  def toAppendStream[T]: DataStream[Row] = {
    this.streamTableEnv.toAppendStream[Row](this.table)
  }

  /**
   * 将Table转为Retract流
   */
  def toRetractStream[T]: DataStream[(Boolean, Row)] = {
    this.streamTableEnv.toRetractStream[Row](this.table)
  }

  /**
   * 将Table转为DataSet
   */
  def toDataSet[T]: DataSet[Row] = {
    ValueUtils.requireNonNull(this.batchTableEnv, "BatchTableEnvironment")
    this.batchTableEnv.toDataSet[Row](this.table)
  }

  /**
   * 将流注册为临时表
   *
   * @param tableName
   * 临时表的表名
   */
  def createOrReplaceTempView(tableName: String): Table = {
    if (this.streamTableEnv != null) {
      this.streamTableEnv.createTemporaryView(tableName, table)
    } else if (this.batchTableEnv != null) {
      this.batchTableEnv.createTemporaryView(tableName, table)
    } else {
      throw new NullPointerException("table environment对象实例为空，请检查")
    }
    table
  }

  /**
   * table的jdbc批量sink操作，该api需用户定义row的取数规则，并与sql中的占位符对等
   *
   * @param sql
   * 增删改sql
   * @param batch
   * 每次sink最大的记录数
   * @param flushInterval
   * 多久flush一次（毫秒）
   * @param keyNum
   * 配置文件中的key后缀
   */
  def jdbcBatchUpdate2(sql: String,
                       batch: Int = 10,
                       flushInterval: Long = 1000,
                       isMerge: Boolean = true,
                       keyNum: Int = 1)(fun: Row => Seq[Any]): DataStreamSink[Row] = {
    if (!isMerge) throw new IllegalArgumentException("该jdbc sink api暂不支持非merge语句，delete操作需单独实现")

    val dstream = this.table.toRetractStream[Row].filter(t => t._1).map(t => t._2)

    import com.zto.fire.flink.core.ext.FlinkExt._
    dstream.jdbcBatchUpdate2(sql, batch, flushInterval, keyNum) {
      row => fun(row)
    }
  }

  /**
   * table的jdbc批量sink操作，根据用户指定的Row中字段的顺序，依次填充到sql中的占位符所对应的位置
   * 注：
   *  1. Row中的字段顺序要与sql中占位符顺序保持一致，数量一致
   *  2. 目前仅处理Retract中的true消息，用户需手动传入merge语句
   *
   * @param sql
   * 增删改sql
   * @param batch
   * 每次sink最大的记录数
   * @param flushInterval
   * 多久flush一次（毫秒）
   * @param keyNum
   * 配置文件中的key后缀
   */
  def jdbcBatchUpdate(sql: String,
                      batch: Int = 10,
                      flushInterval: Long = 1000,
                      isMerge: Boolean = true,
                      keyNum: Int = 1): DataStreamSink[Row] = {

    this.jdbcBatchUpdate2(sql, batch, flushInterval, isMerge, keyNum){
      row => {
        val param = ListBuffer[Any]()
        for (i <- 0 until row.getArity) {
          param += row.getField(i)
        }
        param
      }
    }
  }
}
