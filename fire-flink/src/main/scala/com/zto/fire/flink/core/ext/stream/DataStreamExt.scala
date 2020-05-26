package com.zto.fire.flink.core.ext.stream

import java.lang.reflect.Field

import com.zto.fire.common.bean.HBaseBaseBean
import com.zto.fire.common.bean.ogg.OGGBean
import com.zto.fire.common.util.{DateFormatUtils, GlobalConstants, ReflectionUtils, ValueUtils}
import com.zto.fire.core.util.FireUtils
import com.zto.fire.flink.core.ext.functions.FireMapFunction
import com.zto.fire.flink.core.sink.{FlinkHBaseSink, FlinkJdbcSink}
import com.zto.fire.flink.core.util.FlinkSingletonFactory
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.accumulators.SimpleAccumulator
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

import scala.collection.{JavaConversions, mutable}
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
 * 用于对Flink DataStream的API库扩展
 *
 * @author ChengLong 2020年1月7日 09:18:21
 * @since 0.4.1
 */
class DataStreamExt[T](stream: DataStream[T]) {
  lazy val tableEnv = FlinkSingletonFactory.getStreamTableEnv

  /**
   * 将流注册为临时表
   *
   * @param tableName
   * 临时表的表名
   */
  def createOrReplaceTempView(tableName: String): Table = {
    val table = this.stream.toTable(this.tableEnv)
    this.tableEnv.createTemporaryView(tableName, table)
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
    this.stream.map(new RichMapFunction[T, String] {
      override def open(parameters: Configuration): Unit = {
        this.getRuntimeContext.addAccumulator(name, acc)
      }

      override def map(value: T): String = value.toString
    })
  }

  /**
   * 将流映射为批流
   *
   * @param count
   * 将指定数量的合并为一个集合
   */
  def countWindowSimple[T: ClassTag](count: Long): DataStream[List[T]] = {
    implicit val typeInfo = TypeInformation.of(classOf[List[T]])
    stream.asInstanceOf[DataStream[T]].countWindowAll(Math.abs(count)).apply(new AllWindowFunction[T, List[T], GlobalWindow]() {
      override def apply(window: GlobalWindow, input: Iterable[T], out: Collector[List[T]]): Unit = {
        out.collect(input.toList)
      }
    })(typeInfo)
  }

  /**
   * 设置并行度
   */
  def repartition(parallelism: Int): DataStream[T] = {
    this.stream.setParallelism(parallelism)
  }

  /**
   * 将DataStream转为Table
   */
  def toTable: Table = {
    this.tableEnv.fromDataStream(this.stream)
  }

  /**
   * 解析ogg中的json数据为指定的JavaBean类型
   * 支持消息格式为json和jsonarray
   *
   * @param clazz
   * 目标类型
   * @param paseAfter
   * 是否解析after数据
   * @param paseBefore
   * 是否解析before数据
   * @return
   * 对应类型的DStream
   */
  def mapOgg[E: ClassTag](clazz: Class[E], paseAfter: Boolean = true, paseBefore: Boolean = true): DataStream[OGGBean[E]] = {
    if (!this.stream.isInstanceOf[DataStream[String]]) throw new IllegalArgumentException("ogg消息解析失败：DStream必须为String类型")

    this.stream.flatMap(new FireMapFunction[T, OGGBean[E]]() {
      /**
       * flatMap操作需复写该方法
       */
      override def flatMap(value: T, out: Collector[OGGBean[E]]): Unit = {
        val json = StringUtils.trim(value.asInstanceOf[String])
        if (StringUtils.isNotBlank(json)) {
          if (json.startsWith("[") && json.endsWith("]")) {
            // json array
            val oggList = FireUtils.oggJsonArrayParse(json, clazz, paseAfter, paseBefore)
            if (oggList != null && oggList.size > 0) oggList.filter(ogg => ogg != null).foreach(ogg => out.collect(ogg))
          } else if (json.startsWith("{") && json.endsWith("}")) {
            // json
            val ogg = FireUtils.oggJsonParse(json, clazz, paseAfter, paseBefore)
            if (ogg != null) out.collect(ogg)
          } else {
            throw new IllegalArgumentException("ogg消息解析失败：json格式不合法")
          }
        }
      }
    })
  }

  /**
   * jdbc批量sink操作，根据用户指定的DataStream中字段的顺序，依次填充到sql中的占位符所对应的位置
   * 若DataStream为DataStream[Row]类型，则fields可以为空，但此时row中每列的顺序要与sql占位符顺序一致，数量和类型也要一致
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
  def jdbcBatchUpdate(sql: String,
                      fields: String,
                      batch: Int = 10,
                      flushInterval: Long = 1000,
                      keyNum: Int = 1): DataStreamSink[T] = {
    this.stream.addSink(new FlinkJdbcSink[T](sql, batch = batch, flushInterval = flushInterval, keyNum = keyNum) {
      var fieldMap: java.util.Map[String, Field] = _
      var clazz: Class[_] = _
      var fieldList: Array[String] = _

      override def map(value: T): Seq[Any] = {
        ValueUtils.requireNonNullForce(sql, "sql语句不能为空")

        val params = ListBuffer[Any]()
        if (value.isInstanceOf[Row] || value.isInstanceOf[Tuple2[Boolean, Row]]) {
          // 如果是Row类型的DataStream[Row]
          val row = if (value.isInstanceOf[Row]) value.asInstanceOf[Row] else value.asInstanceOf[Tuple2[Boolean, Row]]._2
          for (i <- 0 until row.getArity) {
            params += row.getField(i)
          }
        } else {
          ValueUtils.requireNonNullForce(fields, "字段列表不能为空！请以逗号分隔，按照sql中的占位符顺序依次指定当前DataStream中数据字段的名称")
          if (this.fieldList == null) this.fieldList = fields.split(",").map(field => StringUtils.trim(field))

          if (clazz == null) {
            if (value != null) {
              clazz = value.getClass
              fieldMap = ReflectionUtils.getAllFields(clazz)
            }
          }

          this.fieldList.foreach(fieldName => {
            val field = this.fieldMap.get(fieldName)
            ValueUtils.requireNonNullForce(field, s"当前DataStream中不存在该列名$fieldName，请检查！")
            params += field.get(value)
          })
        }
        params
      }
    })
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
  def jdbcBatchUpdate2(sql: String,
                       batch: Int = 10,
                       flushInterval: Long = 1000,
                       keyNum: Int = 1)(fun: T => Seq[Any]): DataStreamSink[T] = {
    this.stream.addSink(new FlinkJdbcSink[T](sql, batch = batch, flushInterval = flushInterval, keyNum = keyNum) {
      override def map(value: T): Seq[Any] = {
        fun(value)
      }
    })
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
  def hbaseOperPutDS(tableName: String,
                     insertEmpty: Boolean = true,
                     batch: Int = 100,
                     multiVersion: Boolean = false,
                     flushInterval: Long = 3000,
                     keyNum: Int = 1): DataStreamSink[_] = {

    this.hbaseOperPutDS2(tableName, insertEmpty, batch, multiVersion, flushInterval, keyNum) {
      value => {
        if (!value.isInstanceOf[HBaseBaseBean[T]]) {
          throw new IllegalArgumentException("hbase sink 失败，DataStream中的数据类型必须为DataStream[HBaseBaseBean]")
        }
        value.asInstanceOf[HBaseBaseBean[T]]
      }
    }
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
  def hbaseOperPutDS2(tableName: String,
                      insertEmpty: Boolean = true,
                      batch: Int = 100,
                      multiVersion: Boolean = false,
                      flushInterval: Long = 3000,
                      keyNum: Int = 1)(fun: T => HBaseBaseBean[T]): DataStreamSink[_] = {
    this.stream.addSink(new FlinkHBaseSink[T](tableName, insertEmpty, batch, multiVersion, flushInterval, keyNum) {
      /**
       * 将数据构建成sink的格式
       */
      override def map(value: T): HBaseBaseBean[_] = {
        fun(value)
      }
    })
  }

}
