package com.zto.fire.core.connector

import com.zto.fire.common.conf.FireHBaseConf
import com.zto.fire.common.db.ConnectorFactory
import com.zto.fire.hbase.HBaseConnector
import com.zto.fire.hbase.bean.HBaseBaseBean
import com.zto.fire.hbase.utils.HBaseUtils
import com.zto.fire.predef._
import com.zto.fire.spark.util.{SparkSingletonFactory, SparkUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.client.{Get, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
  * HBase-Spark桥，为Spark提供了使用Java API操作HBase的方式
  *
  * @author ChengLong 2019-5-10 14:39:39
  */
private[fire] class HBaseSparkBridge(keyNum: Int = 1) {
  private[this] lazy val spark = SparkSingletonFactory.getSparkSession
  private[this] lazy val hbaseOper = HBaseConnector(keyNum = this.keyNum)

  def batchSize: Int = FireHBaseConf.hbaseBatchSize()

  /**
    * 使用Java API的方式将DataFrame中的数据分多个批次插入到HBase中
    *
    * @param tableName
    * HBase表名
    * @param df
    * DataFrame
    * @param clazz
    * JavaBean类型，为HBaseBaseBean的子类
    */
  def hbaseOperPutDF[E <: HBaseBaseBean[E] : ClassTag](tableName: String, clazz: Class[E], df: DataFrame): Unit = {
    df.mapPartitions(row => SparkUtils.sparkRowToBean(row, clazz))(Encoders.bean(clazz)).foreachPartition(it => {
      this.multiBatchInsert(tableName, it)
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
    */
  def hbaseOperPutDS[E <: HBaseBaseBean[E] : ClassTag](tableName: String, clazz: Class[E], ds: Dataset[E]): Unit = {
    ds.foreachPartition(it => {
      this.multiBatchInsert(tableName, it)
    })
  }

  /**
    * 使用Java API的方式将RDD中的数据分多个批次插入到HBase中
    *
    * @param tableName
    * HBase表名
    */
  def hbaseOperPutRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, rdd: RDD[T]): Unit = {
    rdd.foreachPartition(it => {
      this.multiBatchInsert(tableName, it)
    })
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
  def hbaseOperScanDF[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], scan: Scan): DataFrame = {
    val beanRDD = this.hbaseOperScanRDD(tableName, clazz, scan)
    // 将rdd转为DataFrame
    this.spark.createDataFrame(beanRDD, clazz)
  }

  /**
    * Scan指定HBase表的数据，并映射为Dataset
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
  def hbaseOperScanDS[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], scan: Scan): Dataset[T] = {
    val beanRDD = this.hbaseOperScanRDD(tableName, clazz, scan)
    // 将rdd转为DataFrame
    spark.createDataset(beanRDD)(Encoders.bean(clazz))
  }

  /**
    * Scan指定HBase表的数据，并映射为Dataset
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
  def hbaseOperScanDS2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], startRow: String, stopRow: String): Dataset[T] = {
    this.hbaseOperScanDS[T](tableName, clazz, HBaseConnector.buildScan(startRow, stopRow))
  }

  /**
    * Scan指定HBase表的数据，并映射为RDD[(ImmutableBytesWritable, Result)]
    *
    * @param tableName
    * HBase表名
    * @param scan
    * scan对象
    * 目标类型
    * @return
    */
  def hbaseHadoopScanRS(tableName: String, scan: Scan): RDD[(ImmutableBytesWritable, Result)] = {
    val hbaseConf = this.hbaseOper.getConfiguration
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
    hbaseConf.set(TableInputFormat.SCAN, HBaseUtils.convertScanToString(scan))
    // 将指定范围内的hbase数据转为rdd
    val resultRDD = this.spark.sparkContext.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]).repartition(FireHBaseConf.hbaseHadoopScanPartitions).persist(StorageLevel.fromString(FireHBaseConf.hbaseStorageLevel))
    resultRDD
  }

  /**
    * Scan指定HBase表的数据，并映射为RDD[(ImmutableBytesWritable, Result)]
    *
    * @param tableName
    * HBase表名
    * @param startRow
    * rowKey开始位置
    * @param stopRow
    * rowKey结束位置
    * 目标类型
    * @return
    */
  def hbaseHadoopScanRS2(tableName: String, startRow: String, stopRow: String): RDD[(ImmutableBytesWritable, Result)] = {
    this.hbaseHadoopScanRS(tableName, HBaseConnector.buildScan(startRow, stopRow))
  }

  /**
    * Scan指定HBase表的数据，并映射为RDD[(T]
    *
    * @param tableName
    * HBase表名
    * @param scan
    * scan对象
    * 目标类型
    * @return
    */
  def hbaseHadoopScanRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], scan: Scan): RDD[T] = {
    val rdd = this.hbaseHadoopScanRS(tableName, scan)
    rdd.mapPartitions(it => HBaseConnector(keyNum = keyNum).hbaseRow2BeanList(it, clazz))
  }

  /**
    * Scan指定HBase表的数据，并映射为RDD[T]
    *
    * @param tableName
    * HBase表名
    * @param startRow
    * rowKey开始位置
    * @param stopRow
    * rowKey结束位置
    * 目标类型
    * @return
    */
  def hbaseHadoopScanRDD2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], startRow: String, stopRow: String): RDD[T] = {
    this.hbaseHadoopScanRDD[T](tableName, clazz, HBaseConnector.buildScan(startRow, stopRow))
  }

  /**
    * Scan指定HBase表的数据，并映射为RDD[(T]
    *
    * @param tableName
    * HBase表名
    * @param scan
    * scan对象
    * 目标类型
    * @return
    */
  def hbaseHadoopScanDF[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], scan: Scan): DataFrame = {
    val rdd = this.hbaseHadoopScanRDD[T](tableName, clazz, scan)
    this.spark.createDataFrame(rdd, clazz)
  }

  /**
    * Scan指定HBase表的数据，并映射为RDD[(ImmutableBytesWritable, Result)]
    *
    * @param tableName
    * HBase表名
    * @param startRow
    * rowKey开始位置
    * @param stopRow
    * rowKey结束位置
    * 目标类型
    * @return
    */
  def hbaseHadoopScanDF2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], startRow: String, stopRow: String): DataFrame = {
    this.hbaseHadoopScanDF[T](tableName, clazz, HBaseConnector.buildScan(startRow, stopRow))
  }

  /**
    * Scan指定HBase表的数据，并映射为RDD[(T]
    *
    * @param tableName
    * HBase表名
    * @param scan
    * scan对象
    * 目标类型
    * @return
    */
  def hbaseHadoopScanDS[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], scan: Scan): Dataset[T] = {
    val rdd = this.hbaseHadoopScanRDD[T](tableName, clazz, scan)
    this.spark.createDataset(rdd)(Encoders.bean(clazz))
  }

  /**
    * Scan指定HBase表的数据，并映射为RDD[(ImmutableBytesWritable, Result)]
    *
    * @param tableName
    * HBase表名
    * @param startRow
    * rowKey开始位置
    * @param stopRow
    * rowKey结束位置
    * 目标类型
    * @return
    */
  def hbaseHadoopScanDS2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], startRow: String, stopRow: String): Dataset[T] = {
    this.hbaseHadoopScanDS[T](tableName, clazz, HBaseConnector.buildScan(startRow, stopRow))
  }

  /**
    * Scan指定HBase表的数据，并映射为RDD[(ImmutableBytesWritable, Result)]
    *
    * @param tableName
    * HBase表名
    * @param startRow
    * rowKey开始位置
    * @param stopRow
    * rowKey结束位置
    * 目标类型
    * @return
    */
  def hbaseOperScanDF2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], startRow: String, stopRow: String): DataFrame = {
    this.hbaseOperScanDF(tableName, clazz, HBaseConnector.buildScan(startRow, stopRow))
  }

  /**
    * Scan指定HBase表的数据，并映射为RDD[(ImmutableBytesWritable, Result)]
    *
    * @param tableName
    * HBase表名
    * @param scan
    * HBase scan对象
    * @return
    */
  def hbaseOperScanRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], scan: Scan): RDD[T] = {
    val hbaseRDD = this.hbaseHadoopScanRS(tableName, scan)
    val scanRDD = hbaseRDD.mapPartitions(it => {
      if (this.hbaseOper.getMultiVersion[T]) {
        HBaseConnector(keyNum = keyNum).hbaseMultiVersionRow2BeanList(it, clazz)
      } else {
        HBaseConnector(keyNum = keyNum).hbaseRow2BeanList(it, clazz)
      }
    }).persist(StorageLevel.fromString(FireHBaseConf.hbaseStorageLevel))
    scanRDD
  }

  /**
    * Scan指定HBase表的数据，并映射为List
    *
    * @param tableName
    * HBase表名
    * @param scan
    * hbase scan对象
    * @param clazz
    * 目标类型
    * @tparam T
    * 目标类型
    * @return
    */
  def hbaseOperScanList[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], scan: Scan): Seq[T] = {
    this.hbaseOper.scan(tableName, clazz, scan)
  }

  /**
    * Scan指定HBase表的数据，并映射为List
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
  def hbaseOperScanList2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], startRow: String, stopRow: String): Seq[T] = {
    this.hbaseOperScanList[T](tableName, clazz, HBaseConnector.buildScan(startRow, stopRow))
  }

  /**
    * 通过RDD[String]批量获取对应的数据（可获取历史版本的记录）
    *
    * @param rowKeyRDD
    * rdd中存放了待查询的rowKey集合
    * @param tableName
    * HBase表名
    * @param clazz
    * 目标类型
    * @tparam T
    * 目标类型
    * @return
    */
  def hbaseOperGetRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], rowKeyRDD: RDD[String]): RDD[T] = {
    val getRDD = rowKeyRDD.mapPartitions(it => {
      val beanList = ListBuffer[T]()
      val getList = ListBuffer[Get]()
      it.foreach(rowKey => {
        if (StringUtils.isNotBlank(rowKey)) {
          val get = new Get(rowKey.getBytes)
            getList += get
            if (getList.size >= this.batchSize) {
              beanList ++= this.hbaseOper.get(tableName, clazz, getList: _*)
              getList.clear()
            }
        }
      })

      if (getList.nonEmpty) {
        beanList ++= this.hbaseOper.get(tableName, clazz, getList: _*)
        getList.clear()
      }
      beanList.iterator
    }).persist(StorageLevel.fromString(FireHBaseConf.hbaseStorageLevel))
    getRDD
  }

  /**
    * 通过RDD[String]批量获取对应的数据（可获取历史版本的记录）
    *
    * @param rowKeyRDD
    * rdd中存放了待查询的rowKey集合
    * @param tableName
    * HBase表名
    * @param clazz
    * 目标类型
    * @tparam T
    * 目标类型
    * @return
    */
  def hbaseOperGetDF[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], rowKeyRDD: RDD[String]): DataFrame = {
    this.spark.createDataFrame(hbaseOperGetRDD(tableName, clazz, rowKeyRDD), clazz)
  }

  /**
    * 通过RDD[String]批量获取对应的数据（可获取历史版本的记录）
    *
    * @param rowKeyRDD
    * rdd中存放了待查询的rowKey集合
    * @param tableName
    * HBase表名
    * @param clazz
    * 目标类型
    * @tparam T
    * 目标类型
    * @return
    */
  def hbaseOperGetDS[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], rowKeyRDD: RDD[String]): Dataset[T] = {
    this.spark.createDataset(hbaseOperGetRDD(tableName, clazz, rowKeyRDD))(Encoders.bean(clazz))
  }

  /**
    * 使用hbase java api方式插入一个集合的数据到hbase表中
    *
    * @param tableName
    * hbase表名
    * @param seq
    * HBaseBaseBean的子类集合
    */
  def hbaseOperPutList[T <: HBaseBaseBean[T] : ClassTag](tableName: String, seq: Seq[T]): Unit = {
    this.hbaseOper.insert[T](tableName, seq: _*)
  }

  /**
    * 根据rowKey查询数据，并转为List[T]
    *
    * @param tableName
    * hbase表名
    * @param seq
    * rowKey集合
    * @param clazz
    * 目标类型
    * get的版本数
    * @return
    * List[T]
    */
  def hbaseOperGetList[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], seq: Seq[Get]): Seq[T] = {
    this.hbaseOper.get[T](tableName, clazz, seq: _*)
  }

  /**
    * 根据rowKey查询数据，并转为List[T]
    *
    * @param tableName
    * hbase表名
    * @param seq
    * rowKey集合
    * @param clazz
    * 目标类型
    * @return
    * List[T]
    */
  def hbaseOperGetList2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, clazz: Class[T], seq: Seq[String]): Seq[T] = {
    val getList = ListBuffer[Get]()
    seq.filter(StringUtils.isNotBlank).foreach(rowKey => {
        getList += new Get(rowKey.getBytes)
    })

    this.hbaseOperGetList[T](tableName, clazz, getList)
  }

   /**
    * 根据rowKey集合批量删除记录
    *
    * @param tableName
    * hbase表名
    * @param rowKeys
    * rowKey集合
    */
  def hbaseOperDeleteList(tableName: String, rowKeys: Seq[String]): Unit = {
    this.hbaseOper.deleteRows(tableName, rowKeys: _*)
  }

  /**
    * 根据RDD[RowKey]批量删除记录
    *
    * @param tableName
    * hbase表名
    * @param rowKeyRDD
    * rowKey集合
    */
  def hbaseOperDeleteRDD(tableName: String, rowKeyRDD: RDD[String]): Unit = {
    rowKeyRDD.foreachPartition(it => {
      val rowKeyList = ListBuffer[String]()
      var count = 0
      it.foreach(rowKey => {
        if (StringUtils.isNotBlank(rowKey)) {
          rowKeyList += rowKey
          count += rowKeyList.size
        }
        if (rowKeyList.size >= batchSize) {
          this.hbaseOper.deleteRows(tableName, rowKeyList: _*)
          rowKeyList.clear()
        }
      })
      if (rowKeyList.nonEmpty) {
        this.hbaseOper.deleteRows(tableName, rowKeyList: _*)
        rowKeyList.clear()
      }
    })
  }

  /**
    * 根据Dataset[RowKey]批量删除记录
    *
    * @param tableName
    * hbase表名
    * @param dataSet
    * rowKey集合
    */
  def hbaseOperDeleteDS(tableName: String, dataSet: Dataset[String]): Unit = {
    this.hbaseOperDeleteRDD(tableName, dataSet.rdd)
  }

  /**
    * 按照指定的批次大小分多个批次插入数据到hbase中
    *
    * @param tableName
    * hbase表名
    * @param iterator
    * 数据集迭代器
    */
  private def multiBatchInsert[E <: HBaseBaseBean[E] : ClassTag](tableName: String, iterator: Iterator[E]): Unit = {
    var count = 0
    val list = ListBuffer[E]()
    iterator.foreach(bean => {
      list += bean
      if (list.size >= batchSize) {
        this.hbaseOper.insert[E](tableName, list: _*)
        count += list.size
        list.clear()
      }
    })
    if (list.nonEmpty) this.hbaseOper.insert[E](tableName, list: _*)
    count += list.size
    list.clear()
  }
}

/**
 * 用于单例构建伴生类HBaseSparkBridge的实例对象
 * 每个HBaseSparkBridge实例使用keyNum作为标识，并且与每个HBase集群一一对应
 */
object HBaseSparkBridge extends ConnectorFactory[HBaseSparkBridge] {

  /**
   * 约定创建connector子类实例的方法
   */
  override protected def create(conf: Any = null, keyNum: Int = 1): HBaseSparkBridge = {
    requireNonEmpty(keyNum)
    val connector = new HBaseSparkBridge(keyNum)
    logger.debug(s"创建HBaseSparkBridge实例成功. keyNum=$keyNum")
    connector
  }
}