package com.zto.bigdata.spark.common.ext

import com.zto.bigdata.spark.common.bean.HBaseBaseBean
import com.zto.bigdata.spark.common.util.SingletonFactory
import org.apache.spark.sql._

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
  def hbaseBulkPutDataset[T <: HBaseBaseBean[T] : ClassTag](tableName: String, insertEmpty: Boolean = true, multiVersion: Boolean = false): Unit = {
    this.hbaseContext.bulkPutDataset[T](tableName, dataset.asInstanceOf[Dataset[T]], insertEmpty, multiVersion)
  }

  /**
    * 使用spark API的方式将DataFrame中的数据分多个批次插入到HBase中
    *
    * @param tableName
    * HBase表名
    */
  def hbaseHadoopPutDataset[T <: HBaseBaseBean[T] : ClassTag](tableName: String, insertEmpty: Boolean = true): Unit = {
    this.hbaseContext.hadoopPutDataset[T](tableName, dataset.asInstanceOf[Dataset[T]], insertEmpty)
  }
}