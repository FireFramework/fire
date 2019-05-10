package com.zto.bigdata.spark.common.db

import com.zto.bigdata.spark.common.bean.HBaseBaseBean
import com.zto.bigdata.spark.common.util.SparkUtils
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import com.zto.bigdata.spark.common.ext.SparkExt._

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
  * HBase-Spark桥，为Spark提供了使用Java API操作HBase的方式
  *
  * @author ChengLong 2019-5-10 14:39:39
  */
object HBaseSparkBridge {
  val batchSize = 10000

  /**
    * 使用Java API的方式将DataFrame中的数据分多个批次插入到HBase中
    *
    * @param tableName
    * HBase表名
    * @param df
    * DataFrame
    * @param clazz
    * JavaBean类型，为HBaseBaseBean的子类
    * @param batchSize
    * 批次大小
    */
  def hbaseInsertDF[E <: HBaseBaseBean[E] : ClassTag](tableName: String, df: DataFrame, clazz: Class[E], batchSize: Int = this.batchSize): Unit = {
    df.mapPartitions(row => SparkUtils.sparkRowToBean(row, clazz))(Encoders.bean(clazz)).foreachPartition(it => {
      this.multiBatchInsert(tableName, it, batchSize)
    })
  }

  /**
    * 使用Java API的方式将Dataset中的数据分多个批次插入到HBase中
    *
    * @param tableName
    * HBase表名
    * @param ds
    * DataSet[E]的具体类型必须为HBaseBaseBean的子类
    * @param clazz
    * JavaBean类型，为HBaseBaseBean的子类
    * @param batchSize
    * 批次大小
    */
  def hbaseInsertDS[E <: HBaseBaseBean[E] : ClassTag](tableName: String, ds: Dataset[E], clazz: Class[E], batchSize: Int = this.batchSize): Unit = {
    ds.foreachPartition(it => {
      this.multiBatchInsert(tableName, it, batchSize)
    })
  }

  /**
    * 使用Java API的方式将RDD中的数据分多个批次插入到HBase中
    *
    * @param tableName
    * HBase表名
    * @param clazz
    * JavaBean类型，为HBaseBaseBean的子类
    * @param batchSize
    * 批次大小
    */
  def hbaseInsertRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, rdd: RDD[_], clazz: Class[T], batchSize: Int = HBaseSparkBridge.batchSize): Unit = {
    val dataFrame = rdd.sparkContext.createSQLContext.createDataFrame(rdd, clazz)
    HBaseSparkBridge.hbaseInsertDF(tableName, dataFrame, clazz, batchSize)
  }

  /**
    * Scan指定HBase表的数据，并映射为DataFrame
    *
    * @param tableName
    * HBase表名
    * @param scan
    * scan对象
    * @param clazz
    * 目标类型
    * @tparam T
    * 目标类型
    * @return
    */
  def hbaseScan2DF[T <: HBaseBaseBean[T] : ClassTag](spark: SparkSession, tableName: String, scan: Scan, clazz: Class[T]): DataFrame = {
    val hbaseConf = HBaseOper.getConfiguration
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
    hbaseConf.set(TableInputFormat.SCAN, SparkUtils.convertScanToString(scan))
    // 将指定范围内的hbase数据转为rdd
    val hbaseRDD = spark.sparkContext.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]).repartition(1200)
    // 将hbaserdd转为自定义bean类型的rdd
    val beanRDD = hbaseRDD.mapPartitions(it => HBaseOper.hbaseRow2BeanList(it, clazz))
    // 使用spark sql对hbase中的数据进行sql分析
    spark.createDataFrame(beanRDD, clazz)
  }

  /**
    * Scan指定HBase表的数据，并映射为DataFrame
    *
    * @param tableName
    *                HBase表名
    * @param startRow
    *                开始主键
    * @param stopRow 结束主键
    * @param clazz
    *                目标类型
    * @tparam T
    * 目标类型
    * @return
    */
  def hbaseScan2DF[T <: HBaseBaseBean[T] : ClassTag](spark: SparkSession, tableName: String, startRow: String, stopRow: String, clazz: Class[T]): DataFrame = {
    this.hbaseScan2DF(spark, tableName, HBaseOper.buildScan(startRow, stopRow, null), clazz)
  }

  /**
    * 按照指定的批次大小分多个批次插入数据到hbase中
    *
    * @param tableName
    * hbase表名
    * @param iterator
    * 数据集迭代器
    * @param batchSize
    * 批次大小
    * @tparam E HBaseBaseBean的子类
    */
  def multiBatchInsert[E <: HBaseBaseBean[E] : ClassTag](tableName: String, iterator: Iterator[E], batchSize: Int = this.batchSize): Unit = {
    val list = ListBuffer[E]()
    iterator.foreach(bean => {
      list += bean
      if (list.size >= batchSize) {
        HBaseOper.insert(tableName, list)
        list.clear()
      }
    })
    HBaseOper.insert(tableName, list)
  }
}
