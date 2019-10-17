package com.zto.fire.core.ext.core

import com.alibaba.fastjson.JSON
import com.zto.fire.common.bean.HBaseBaseBean
import com.zto.fire.core.bridge.HBaseSparkBridge
import com.zto.fire.core.ext.module.HBaseContextExt
import com.zto.fire.core.util.SingletonFactory
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.Trigger
import com.zto.fire.core.ext.SparkExt._

import scala.collection.JavaConversions
import scala.collection.mutable.ListBuffer
import scala.reflect._

/**
  * Dataset扩展
  *
  * @param dataset
  * dataset对象
  * @author ChengLong 2019-5-18 11:02:56
  */
class DatasetExt[T: ClassTag](dataset: Dataset[T]) {

  // 获取单例的HBaseContext对象
  private lazy val hbaseContext: HBaseContextExt = SingletonFactory.getHBaseContextInstance(dataset.sparkSession.sparkContext)

  /**
    * 打印Dataset的值
    *
    * @param lines
    * 打印的行数
    * @return
    */
  def showString(lines: Int = 100): String = {
    val showLines = if (lines <= 1000) lines else 1000
    val showStringMethod = dataset.getClass.getDeclaredMethod("showString", classOf[Int], classOf[Int], classOf[Boolean])
    showStringMethod.invoke(dataset, new Integer(showLines), new Integer(Int.MaxValue), new java.lang.Boolean(false)).toString
  }

  /**
    * 批量写入，将自定义的JavaBean数据集批量并行写入
    * 到HBase的指定表中。内部会将自定义JavaBean的相应
    * 字段一一映射为Put对象，并完成一次写入
    *
    * @param tableName
    * HBase表名
    * @param insertEmpty
    * 对象中值为空的字段是否覆盖HBase中已有的field值
    * 默认为覆盖
    * @tparam T
    * 数据类型为HBaseBaseBean的子类
    */
  def hbaseBulkPutDS[T <: HBaseBaseBean[T] : ClassTag](tableName: String, insertEmpty: Boolean = true, multiVersion: Boolean = false): Unit = {
    this.hbaseContext.bulkPutDS[T](tableName, dataset.asInstanceOf[Dataset[T]], insertEmpty, multiVersion)
  }

  /**
    * 根据Dataset[String]批量删除，Dataset是rowkey的集合
    * 类型为String
    *
    * @param tableName
    * HBase表名
    * @param batchSize
    * 批量删除的大小，默认为1000条
    */
  def hbaseBulkDeleteDS(tableName: String, batchSize: Integer = this.hbaseContext.batchSize): Unit = {
    this.hbaseContext.bulkDeleteDS(tableName, dataset.asInstanceOf[Dataset[String]], batchSize)
  }

  /**
    * 根据Dataset[RowKey]批量删除记录
    *
    * @param tableName
    * rowKey集合
    * @param batchSize
    * 一次删除多少条
    */
  def hbaseOperDeleteDS(tableName: String, batchSize: Int = this.hbaseContext.batchSize): Unit = {
    HBaseSparkBridge.hbaseOperDeleteDS(tableName, dataset.asInstanceOf[Dataset[String]], batchSize)
  }

  /**
    * 使用spark API的方式将DataFrame中的数据分多个批次插入到HBase中
    *
    * @param tableName
    * HBase表名
    */
  def hbaseHadoopPutDS[T <: HBaseBaseBean[T] : ClassTag](tableName: String, insertEmpty: Boolean = true): Unit = {
    this.hbaseContext.hadoopPutDS[T](tableName, dataset.asInstanceOf[Dataset[T]], insertEmpty)
  }

  /**
    * 使用Java API的方式将Dataset中的数据分多个批次插入到HBase中
    *
    * @param tableName
    * HBase表名
    * @param clazz
    * JavaBean类型，为HBaseBaseBean的子类
    * @param batchSize
    * 批次大小
    * @param multiVersion
    * 是否以多版本方式插入（会将多列数据转为一列的json数据进行保存）
    */
  def hbaseOperPutDS[E <: HBaseBaseBean[E] : ClassTag](tableName: String, clazz: Class[E], insertEmpty: Boolean = true, batchSize: Int = HBaseSparkBridge.batchSize, multiVersion: Boolean = false): Unit = {
    HBaseSparkBridge.hbaseOperPutDS[E](tableName, dataset.asInstanceOf[Dataset[E]], clazz, insertEmpty, batchSize, multiVersion)
  }

  /**
    * 清空RDD的缓存
    */
  def uncache: Unit = {
    dataset.unpersist
  }

  /**
   * 将当前Dataset记录打印到控制台
   */
  def print(outputMode: String = "append", trigger: Trigger = null, numRows: Int = 20, truncate: Boolean = true): Dataset[T] = {
    if (dataset.isStreaming) {
      val tmpStream = dataset.writeStream.outputMode(outputMode).option("numRows", numRows).option("truncate", truncate).format("console")
      if (trigger != null) tmpStream.trigger(trigger)
      tmpStream.start
    } else {
      dataset.show(numRows, truncate)
    }
    dataset
  }

  // ------------------------------------- json 解析 ------------------------------------- //
/*
  /**
   * 通用的消息解析
   *
   * @param parse
   * 消息的处理方式
   * @tparam E
   * 当前消息的类型
   * @return
   * 解析后的Dataset
   */
  def parseMsg[E: ClassTag](parse: (Seq[T]) => Seq[E], batch: Int = 1000): Dataset[E] = {
    val tClass = classTag[E].runtimeClass.asInstanceOf[Class[E]]
    this.dataset.mapPartitions(it => {
      val list = ListBuffer[E]()
      val batchList = ListBuffer[T]()

      it.foreach(t => {
        batchList += t
        if (batchList.size >= batch) {
          val parseList = parse(batchList)
          if (parseList != null && parseList.size > 0) {
            list ++= parseList
          }
          batchList.clear()
        }
      })

      if (batchList.size > 0) {
        val parseList = parse(batchList)
        if (parseList != null && parseList.size > 0) {
          list ++= parseList
        }
        batchList.clear()
      }
      list.iterator
    })(Encoders.bean(tClass))
  }

  /**
   * 用于解析json消息
   *
   * @param schema
   * 解析后的格式
   * @param batch
   * 用于指定一次解析多少条消息
   * @return
   * 解析json后的数据集
   */
  def parseJson[E: ClassTag](schema: Class[E], batch: Int = 2): Dataset[E] = {
    def parse(jsons: Seq[T]): ListBuffer[E] = {
      val list = ListBuffer[E]()
      if (jsons != null && jsons.size > 0) {
        val header = jsons(0).toString
        // json array
        if (StringUtils.isNotBlank(header) && header.contains("[") && header.contains("]")) {
          jsons.map(t => t.toString).foreach(jsonArray => {
            if (StringUtils.isNotBlank(jsonArray)) {
              println(jsonArray)
              list ++= JavaConversions.asScalaBuffer(JSON.parseArray(jsonArray, schema))
            }
          })
        } else {
          // json
          val jsonArray = new StringBuilder("[")
          jsons.map(t => t.toString).foreach(json => {
            if (StringUtils.isNotBlank(json)) {
              jsonArray.append(json)
            }
          })
          jsonArray.append("]")
          list ++= JavaConversions.asScalaBuffer(JSON.parseArray(jsonArray.toString(), schema))
        }
      }
      list
    }
    this.dataset.parseMsg[E](parse _, batch)
  }*/
}