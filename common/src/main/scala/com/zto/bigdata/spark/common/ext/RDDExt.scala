package com.zto.bigdata.spark.common.ext

import com.zto.bigdata.spark.common.bean.HBaseBaseBean
import com.zto.bigdata.spark.common.db.HBaseSparkBridge
import com.zto.bigdata.spark.common.util.SingletonFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.reflect.{ClassTag, classTag}

/**
  * RDD相关扩展
  *
  * @author ChengLong 2019-5-18 10:28:31
  */
class RDDExt[T: ClassTag](rdd: RDD[T]) {
  // 获取单例的HBaseContext对象
  private lazy val hbaseContext: HBaseContextExt = SingletonFactory.getHBaseContextInstance(rdd.sparkContext)

  /**
    * 遍历每个partition并打印元素到控制台
    */
  def printEachPartition: Unit = {
    rdd.foreachPartition(it => {
      it.foreach(item => print(item + " "))
    })
  }

  /**
    * 集群模式下打印数据
    */
  def printEachClusterPartition: Unit = {
    rdd.collect().foreach(println)
  }

  /**
    * 将rdd转为DataFrame
    */
  def rdd2DataFrame(): DataFrame = {
    lazy val hiveContext = SingletonFactory.getSQLContextInstance(rdd.sparkContext)
    hiveContext.createDataFrame(rdd, classTag[T].runtimeClass)
  }

  /**
    * 将rdd转为DataFrame并注册成临时表
    *
    * @param tableName
    * 表名
    * @return
    * DataFrame
    */
  def rddRegisterTableAndCache(tableName: String): DataFrame = {
    lazy val hiveContext = SingletonFactory.getSQLContextInstance(rdd.sparkContext)
    val dataFrame = this.rdd2DataFrame()
    dataFrame.createOrReplaceTempView(tableName)
    hiveContext.cacheTable(tableName)
    dataFrame
  }

  /**
    * 根据RDD[String]批量删除
    *
    * @param tableName
    * HBase表名
    * @param batchSize
    * 批量删除的大小
    */
  def hbaseBulkDelete(tableName: String, batchSize: Integer = this.hbaseContext.batchSize): Unit = {
    this.hbaseContext.bulkDelete(tableName, rdd.asInstanceOf[RDD[String]], batchSize)
  }

  /**
    * 根据rowKey集合批量获取数据
    *
    * @param tableName
    * HBase表名
    * @param clazz
    * 获取后的记录转换为目标类型
    * @param batchSize
    * 批量的大小
    * @return
    * 结果集
    */
  def hbaseBulkGet[E <: HBaseBaseBean[E] : ClassTag](tableName: String, clazz: Class[E], batchSize: Integer = this.hbaseContext.batchSize): RDD[HBaseBaseBean[E]] = {
    this.hbaseContext.bulkGet(tableName, rdd.asInstanceOf[RDD[String]], clazz, batchSize)
  }

  /**
    * 批量插入数据
    *
    * @param tableName
    * HBase表名
    * 数据集合，继承自HBaseBaseBean
    * @param insertEmpty
    * 为空的字段是否写入
    */
  def hbaseBulkPut[T <: HBaseBaseBean[T] : ClassTag](tableName: String, insertEmpty: Boolean = true, multiVersion: Boolean = false): Unit = {
    this.hbaseContext.bulkPut(tableName, rdd.asInstanceOf[RDD[T]], insertEmpty, multiVersion)
  }

  /**
    * 使用Spark API的方式将RDD中的数据分多个批次插入到HBase中
    *
    * @param tableName
    * HBase表名
    */
  def hbaseHadoopPut[T <: HBaseBaseBean[T] : ClassTag](tableName: String, insertEmpty: Boolean = true): Unit = {
    this.hbaseContext.hadoopPut(tableName, rdd.asInstanceOf[RDD[T]], insertEmpty)
  }

  /**
    * 通过RDD[String]批量获取对应的数据（可获取历史版本的记录）
    *
    * @param tableName
    * HBase表名
    * @param clazz
    * 目标类型
    * @param multiVersion
    * 是否以多版本方式插入（会将多列数据转为一列的json数据进行保存）
    * @tparam T
    * 目标类型
    * @return
    */
  def hbaseOperGetRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): RDD[T] = {
    HBaseSparkBridge.hbaseOperGetRDD(tableName, rdd.asInstanceOf[RDD[String]], clazz, multiVersion, versions)
  }

  /**
    * 通过RDD[String]批量获取对应的数据（可获取历史版本的记录）
    *
    * @param tableName
    * HBase表名
    * @param clazz
    * 目标类型
    * @param multiVersion
    * 是否以多版本方式插入（会将多列数据转为一列的json数据进行保存）
    * @tparam T
    * 目标类型
    * @return
    */
  def hbaseOperGetDF[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): DataFrame = {
    HBaseSparkBridge.hbaseOperGetDF(tableName, rdd.asInstanceOf[RDD[String]], clazz, multiVersion, versions)
  }

  /**
    * 使用Java API的方式将RDD中的数据分多个批次插入到HBase中
    *
    * @param tableName
    * HBase表名
    * @param batchSize
    * 批次大小
    * @param multiVersion
    * 是否以多版本方式插入（会将多列数据转为一列的json数据进行保存）
    */
  def hbaseOperInsertRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, insertEmpty: Boolean = true, batchSize: Int = HBaseSparkBridge.batchSize, multiVersion: Boolean = false): Unit = {
    HBaseSparkBridge.hbaseOperInsertRDD[T](tableName, rdd.asInstanceOf[RDD[T]], insertEmpty, batchSize, multiVersion)
  }

}