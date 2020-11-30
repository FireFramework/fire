package com.zto.fire.flink.core.ext.stream

import com.zto.fire.common.bean.HBaseBaseBean
import com.zto.fire.core.bridge.JdbcOperBridge
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.Table
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row
import com.zto.fire.flink.core.ext.FlinkExt._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * 用于对Flink StreamTableEnvironment的API库扩展
 *
 * @author ChengLong 2020年1月7日 09:18:21
 * @since 0.4.1
 */
class StreamTableEnvExt(tableEnv: StreamTableEnvironment) extends JdbcOperBridge {

  /**
   * 执行sql query操作
   *
   * @param sql
   * sql语句
   * @return
   * table对象
   */
  def sql(sql: String): Table = {
    this.tableEnv.sqlQuery(sql)
  }

  /**
   * 注册自定义udf函数
   *
   * @param name
   * 函数名
   * @param function
   * 函数的实例
   */
  def udf(name: String, function: ScalarFunction): Unit = {
    this.tableEnv.registerFunction(name, function)
  }

  /**
   * jdbc批量sink操作，根据用户指定的DataStream中字段的顺序，依次填充到sql中的占位符所对应的位置
   * 注：
   *  1. fieldList指定DataStream中JavaBean的字段名称，非jdbc表中的字段名称
   *  2. fieldList多个字段使用逗号分隔
   *  3. fieldList中的字段顺序要与sql中占位符顺序保持一致，数量一致
   *
   * @param sql
   * 增删改sql
   * @param fields
   * DataStream中数据的每一列的列名（非数据库中的列名，需与sql中占位符的顺序一致）
   * @param batch
   * 每次sink最大的记录数
   * @param flushInterval
   * 多久flush一次（毫秒）
   * @param keyNum
   * 配置文件中的key后缀
   */
  def jdbcBatchUpdateStream[T](stream: DataStream[T],
                               sql: String,
                               fields: Seq[String],
                               batch: Int = 10,
                               flushInterval: Long = 1000,
                               keyNum: Int = 1): DataStreamSink[T] = {
    stream.jdbcBatchUpdate(sql, fields, batch, flushInterval, keyNum)
  }

  /**
   * jdbc批量sink操作
   *
   * @param sql
   * 增删改sql
   * @param batch
   * 每次sink最大的记录数
   * @param flushInterval
   * 多久flush一次（毫秒）
   * @param keyNum
   * 配置文件中的key后缀
   * @param fun
   * 将dstream中的数据映射为该sink组件所能处理的数据
   */
  def jdbcBatchUpdateStream2[T](stream: DataStream[T],
                                sql: String,
                                batch: Int = 10,
                                flushInterval: Long = 1000,
                                keyNum: Int = 1)(fun: T => Seq[Any]): DataStreamSink[T] = {
    stream.jdbcBatchUpdate2(sql, batch, flushInterval, keyNum)(fun)
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
  def jdbcBatchUpdateTable(table: Table,
                           sql: String,
                           batch: Int = 10,
                           flushInterval: Long = 1000,
                           isMerge: Boolean = true,
                           keyNum: Int = 1): DataStreamSink[Row] = {
    table.jdbcBatchUpdate(sql, batch, flushInterval, isMerge, keyNum)
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
  def jdbcBatchUpdateTable2(table: Table,
                            sql: String,
                            batch: Int = 10,
                            flushInterval: Long = 1000,
                            isMerge: Boolean = true,
                            keyNum: Int = 1)(fun: Row => Seq[Any]): DataStreamSink[Row] = {
    table.jdbcBatchUpdate2(sql, batch, flushInterval, isMerge, keyNum)(fun)
  }

  /**
   * hbase批量sink操作，DataStream[T]中的T必须是HBaseBaseBean的子类
   *
   * @param tableName
   * hbase表名
   * @param insertEmpty
   * 为空的字段是否插入到hbase中
   * @param batch
   * 每次sink最大的记录数
   * @param multiVersion
   * 是否以多版本形式保存
   * @param flushInterval
   * 多久flush一次（毫秒）
   * @param keyNum
   * 配置文件中的key后缀
   */
  def hbaseOperPutDS[T <: HBaseBaseBean[T]](stream: DataStream[T],
                                            tableName: String,
                                            insertEmpty: Boolean = true,
                                            batch: Int = 100,
                                            multiVersion: Boolean = false,
                                            flushInterval: Long = 3000,
                                            keyNum: Int = 1): DataStreamSink[_] = {
    stream.hbaseOperPutDS(tableName, insertEmpty, batch, multiVersion, flushInterval, keyNum)
  }

  /**
   * hbase批量sink操作，DataStream[T]中的T必须是HBaseBaseBean的子类
   *
   * @param tableName
   * hbase表名
   * @param insertEmpty
   * 为空的字段是否插入到hbase中
   * @param batch
   * 每次sink最大的记录数
   * @param multiVersion
   * 是否以多版本形式保存
   * @param flushInterval
   * 多久flush一次（毫秒）
   * @param keyNum
   * 配置文件中的key后缀
   * @param fun
   * 将dstream中的数据映射为该sink组件所能处理的数据
   */
  def hbaseOperPutDS2[T](stream: DataStream[T],
                         tableName: String,
                         insertEmpty: Boolean = true,
                         batch: Int = 100,
                         multiVersion: Boolean = false,
                         flushInterval: Long = 3000,
                         keyNum: Int = 1)(fun: T => HBaseBaseBean[T]): DataStreamSink[_] = {
    stream.hbaseOperPutDS2(tableName, insertEmpty, batch, multiVersion, flushInterval, keyNum)(fun)
  }

  /**
   * table的hbase批量sink操作，该api需用户定义row的取数规则，并映射到对应的HBaseBaseBean的子类中
   *
   * @param tableName
   *                     HBase表名
   * @param insertEmpty  为空的字段是否插入
   * @param batch
   *                     每次sink最大的记录数
   * @param multiVersion 是否以多版本方式写入
   * @param flushInterval
   *                     多久flush一次（毫秒）
   * @param keyNum
   *                     配置文件中的key后缀
   */
  def hbaseOperPutTable[T <: HBaseBaseBean[T]](table: Table,
                                               tableName: String,
                                               clazz: Class[T],
                                               insertEmpty: Boolean = true,
                                               batch: Int = 100,
                                               multiVersion: Boolean = false,
                                               flushInterval: Long = 3000,
                                               keyNum: Int = 1): DataStreamSink[_] = {
    table.hbaseOperPutTable[T](tableName, clazz, insertEmpty, batch, multiVersion, flushInterval, keyNum)
  }

  /**
   * table的hbase批量sink操作，该api需用户定义row的取数规则，并映射到对应的HBaseBaseBean的子类中
   *
   * @param tableName
   *                     HBase表名
   * @param insertEmpty  为空的字段是否插入
   * @param batch
   *                     每次sink最大的记录数
   * @param multiVersion 是否以多版本方式写入
   * @param flushInterval
   *                     多久flush一次（毫秒）
   * @param keyNum
   *                     配置文件中的key后缀
   */
  def hbaseOperPutTable2(table: Table,
                         tableName: String,
                         insertEmpty: Boolean = true,
                         batch: Int = 100,
                         multiVersion: Boolean = false,
                         flushInterval: Long = 3000,
                         keyNum: Int = 1)(fun: Row => HBaseBaseBean[_]): DataStreamSink[_] = {
    table.hbaseOperPutTable2(tableName, insertEmpty, batch, multiVersion, flushInterval, keyNum)(fun)
  }
}
