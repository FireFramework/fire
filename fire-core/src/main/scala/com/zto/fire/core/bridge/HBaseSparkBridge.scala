package com.zto.fire.core.bridge

import java.util

import com.zto.fire.common.bean.HBaseBaseBean
import com.zto.fire.common.conf.FireHBaseConf
import com.zto.fire.common.db.HBaseOper
import com.zto.fire.common.util.HBaseUtils
import com.zto.fire.core.ext.SparkExt._
import com.zto.fire.core.util.{SingletonFactory, SparkUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.client.{Get, Result, Scan}
import org.apache.hadoop.hbase.filter.{Filter, FilterList}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConversions
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
  * HBase-Spark桥，为Spark提供了使用Java API操作HBase的方式
  *
  * @author ChengLong 2019-5-10 14:39:39
  */
object HBaseSparkBridge extends HBaseOper with Logging {
  private[this] lazy val sparkSession = SingletonFactory.getSparkSession

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
    * @param batchSize
    * 批次大小
    * @param multiVersion
    * 是否以多版本方式插入（会将多列数据转为一列的json数据进行保存）
    */
  def hbaseOperPutDF[E <: HBaseBaseBean[E] : ClassTag](tableName: String, df: DataFrame, clazz: Class[E], insertEmpty: Boolean = true, batchSize: Int = this.batchSize, multiVersion: Boolean = false): Unit = {
    df.mapPartitions(row => SparkUtils.sparkRowToBean(row, clazz))(Encoders.bean(clazz)).foreachPartition(it => {
      this.multiBatchInsert(tableName, it, insertEmpty, batchSize, multiVersion)
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
    * @param multiVersion
    * 是否以多版本方式插入（会将多列数据转为一列的json数据进行保存）
    */
  def hbaseOperPutDS[E <: HBaseBaseBean[E] : ClassTag](tableName: String, ds: Dataset[E], clazz: Class[E], insertEmpty: Boolean = true, batchSize: Int = this.batchSize, multiVersion: Boolean = false): Unit = {
    ds.foreachPartition(it => {
      this.multiBatchInsert(tableName, it, insertEmpty, batchSize, multiVersion)
    })
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
  def hbaseOperPutRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, rdd: RDD[T], insertEmpty: Boolean = true, batchSize: Int = HBaseSparkBridge.batchSize, multiVersion: Boolean = false): Unit = {
    rdd.foreachPartition(it => {
      this.multiBatchInsert(tableName, it, insertEmpty, batchSize, multiVersion)
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
  def hbaseOperScanDF[T <: HBaseBaseBean[T] : ClassTag](spark: SparkSession, tableName: String, scan: Scan, clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): DataFrame = {
    val beanRDD = this.hbaseOperScanRDD(spark, tableName, scan, clazz, multiVersion, versions)
    // 将rdd转为DataFrame
    spark.createDataFrame(beanRDD, clazz)
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
  def hbaseOperScanDS[T <: HBaseBaseBean[T] : ClassTag](spark: SparkSession, tableName: String, scan: Scan, clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): Dataset[T] = {
    val beanRDD = this.hbaseOperScanRDD(spark, tableName, scan, clazz, multiVersion, versions)
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
  def hbaseOperScanDS2[T <: HBaseBaseBean[T] : ClassTag](spark: SparkSession, tableName: String, startRow: String, stopRow: String, clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): Dataset[T] = {
    this.hbaseOperScanDS[T](spark, tableName, HBaseOper.buildScan(startRow, stopRow), clazz, multiVersion, versions)
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
  def hbaseHadoopScanRS(spark: SparkSession, tableName: String, scan: Scan, multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): RDD[(ImmutableBytesWritable, Result)] = {
    if (multiVersion) scan.setMaxVersions(versions)
    val hbaseConf = HBaseOper.getConfiguration
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
    hbaseConf.set(TableInputFormat.SCAN, HBaseUtils.convertScanToString(scan))
    // 将指定范围内的hbase数据转为rdd
    val resultRDD = spark.sparkContext.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]).repartition(FireHBaseConf.hbaseHadoopScanPartitions).persist(StorageLevel.fromString(FireHBaseConf.hbaseStorageLevel))
    this.logFire(s"hbaseHadoopScanRS(tableName: $tableName)", "hbase", 1)
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
  def hbaseHadoopScanRS2(spark: SparkSession, tableName: String, startRow: String, stopRow: String, multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): RDD[(ImmutableBytesWritable, Result)] = {
    this.hbaseHadoopScanRS(spark, tableName, HBaseOper.buildScan(startRow, stopRow), multiVersion, versions)
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
  def hbaseHadoopScanRDD[T <: HBaseBaseBean[T] : ClassTag](spark: SparkSession, tableName: String, scan: Scan, clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): RDD[T] = {
    val rdd = this.hbaseHadoopScanRS(spark, tableName, scan, multiVersion, versions)
    rdd.mapPartitions(it => HBaseOper.hbaseRow2BeanList(it, clazz))
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
  def hbaseHadoopScanRDD2[T <: HBaseBaseBean[T] : ClassTag](spark: SparkSession, tableName: String, startRow: String, stopRow: String, clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): RDD[T] = {
    this.hbaseHadoopScanRDD[T](spark, tableName, HBaseOper.buildScan(startRow, stopRow), clazz, multiVersion, versions)
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
  def hbaseHadoopScanDF[T <: HBaseBaseBean[T] : ClassTag](spark: SparkSession, tableName: String, scan: Scan, clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): DataFrame = {
    val rdd = this.hbaseHadoopScanRDD[T](spark, tableName, scan, clazz, multiVersion, versions)
    spark.createDataFrame(rdd, clazz)
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
  def hbaseHadoopScanDF2[T <: HBaseBaseBean[T] : ClassTag](spark: SparkSession, tableName: String, startRow: String, stopRow: String, clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): DataFrame = {
    this.hbaseHadoopScanDF[T](spark, tableName, HBaseOper.buildScan(startRow, stopRow), clazz, multiVersion, versions)
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
  def hbaseHadoopScanDS[T <: HBaseBaseBean[T] : ClassTag](spark: SparkSession, tableName: String, scan: Scan, clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): Dataset[T] = {
    val rdd = this.hbaseHadoopScanRDD[T](spark, tableName, scan, clazz, multiVersion, versions)
    spark.createDataset(rdd)(Encoders.bean(clazz))
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
  def hbaseHadoopScanDS2[T <: HBaseBaseBean[T] : ClassTag](spark: SparkSession, tableName: String, startRow: String, stopRow: String, clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): Dataset[T] = {
    this.hbaseHadoopScanDS[T](spark, tableName, HBaseOper.buildScan(startRow, stopRow), clazz, multiVersion, versions)
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
  def hbaseOperScanDF2[T <: HBaseBaseBean[T] : ClassTag](spark: SparkSession, tableName: String, startRow: String, stopRow: String, clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): DataFrame = {
    this.hbaseOperScanDF(spark, tableName, HBaseOper.buildScan(startRow, stopRow), clazz, multiVersion, versions)
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
  def hbaseOperScanRDD[T <: HBaseBaseBean[T] : ClassTag](spark: SparkSession, tableName: String, scan: Scan, clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): RDD[T] = {
    this.mark
    val hbaseRDD = this.hbaseHadoopScanRS(spark, tableName, scan, multiVersion, versions)
    val scanRDD = hbaseRDD.mapPartitions(it => {
      if (multiVersion) {
        JavaConversions.asScalaBuffer(HBaseOper.hbaseMultiVersionRow2BeanList(it, clazz)).iterator
      } else {
        HBaseOper.hbaseRow2BeanList(it, clazz)
      }
    }).persist(StorageLevel.fromString(FireHBaseConf.hbaseStorageLevel))
    this.logFire(s"hbaseOperScanRDD(tableName: $tableName)", "hbase", 1)
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
  def hbaseOperScanList[T <: HBaseBaseBean[T] : ClassTag](tableName: String, scan: Scan, clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): Seq[T] = {
    this.mark
    val list = if (multiVersion) {
      scan.setMaxVersions(versions)
      HBaseOper.scanMultiVersions(tableName, scan, clazz)
    } else {
      HBaseOper.scan(tableName, scan, clazz)
    }
    if (list == null) {
      return null
    }
    this.logFire(s"hbaseOperScanList(tableName: ${tableName})  count: ${list.size}", "hbase", 1)
    JavaConversions.asScalaBuffer(list)
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
  def hbaseOperScanList2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, startRow: String, stopRow: String, clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE, operator: FilterList.Operator = null, filters: Filter = null): Seq[T] = {
    val scan = new Scan(startRow.getBytes, stopRow.getBytes)
    if (filters != null) {
      val filterList = new FilterList(operator, filters)
      scan.setFilter(filterList)
    }
    this.hbaseOperScanList[T](tableName, scan, clazz, multiVersion, versions)
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
    * @param multiVersion
    * 是否以多版本方式插入（会将多列数据转为一列的json数据进行保存）
    * @tparam T
    * 目标类型
    * @return
    */
  def hbaseOperGetRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, rowKeyRDD: RDD[String], clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): RDD[T] = {
    this.mark
    val getRDD = rowKeyRDD.mapPartitions(it => {
      val beanList = new util.LinkedList[T]()
      val getList = new util.LinkedList[Get]()
      this.mark
      it.foreach(rowKey => {
        if (StringUtils.isNotBlank(rowKey)) {
          val get = new Get(rowKey.getBytes)
          if (multiVersion) {
            // 多版本表记录
            get.setMaxVersions(versions)
            getList.add(get)
            if (getList.size >= this.batchSize) {
              val resultList = HBaseOper.getMultiVersions(tableName, getList, clazz)
              if (resultList != null && resultList.size() > 0) {
                beanList.addAll(resultList)
              }
              getList.clear()
            }
          } else {
            // 单版本表记录
            getList.add(get)
            if (getList.size >= this.batchSize) {
              val resultList = HBaseOper.get(tableName, getList, clazz)
              if (resultList != null && resultList.size() > 0) {
                beanList.addAll(resultList)
              }
              getList.clear()
            }
          }
        }
      })

      if (getList.size() > 0) {
        if (multiVersion) {
          val resultList = HBaseOper.getMultiVersions(tableName, getList, clazz)
          if (resultList != null && resultList.size() > 0) {
            beanList.addAll(resultList)
          }
          getList.clear()
        } else {
          val resultList = HBaseOper.get(tableName, getList, clazz)
          if (resultList != null && resultList.size() > 0) {
            beanList.addAll(resultList)
          }
          getList.clear()
        }
      }
      this.logFire(s"hbaseOperGetRDD(tableName: ${tableName}) count: ${beanList.size}", "hbase", 1)
      JavaConversions.asScalaIterator(beanList.iterator())
    }).persist(StorageLevel.fromString(FireHBaseConf.hbaseStorageLevel))
    this.logFire(s"hbaseOperGetRDD(tableName: ${tableName})", "hbase", 1)
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
    * @param multiVersion
    * 是否以多版本方式插入（会将多列数据转为一列的json数据进行保存）
    * @tparam T
    * 目标类型
    * @return
    */
  def hbaseOperGetDF[T <: HBaseBaseBean[T] : ClassTag](tableName: String, rowKeyRDD: RDD[String], clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): DataFrame = {
    this.sparkSession.createDataFrame(hbaseOperGetRDD(tableName, rowKeyRDD, clazz, multiVersion, versions), clazz)
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
    * @param multiVersion
    * 是否以多版本方式插入（会将多列数据转为一列的json数据进行保存）
    * @tparam T
    * 目标类型
    * @return
    */
  def hbaseOperGetDS[T <: HBaseBaseBean[T] : ClassTag](tableName: String, rowKeyRDD: RDD[String], clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): Dataset[T] = {
    this.sparkSession.createDataset(hbaseOperGetRDD(tableName, rowKeyRDD, clazz, multiVersion, versions))(Encoders.bean(clazz))
  }

  /**
    * 使用hbase java api方式插入一个集合的数据到hbase表中
    *
    * @param tableName
    * hbase表名
    * @param seq
    * HBaseBaseBean的子类集合
    * @param insertEmpty
    * 是否插入为空的字段
    * @param multiVersion
    * 是否以多版本形式插入
    */
  def hbaseOperPutList[T <: HBaseBaseBean[T] : ClassTag](tableName: String, seq: Seq[T], insertEmpty: Boolean = true, multiVersion: Boolean = false): Unit = {
    this.mark
    if (multiVersion) {
      HBaseOper.insertMultiVersions(tableName, JavaConversions.seqAsJavaList(seq))
    } else {
      if (insertEmpty) {
        HBaseOper.insert(tableName, JavaConversions.seqAsJavaList(seq))
      } else {
        HBaseOper.insertIgnoreNull(tableName, seq.toList)
      }
    }
    this.logFire(s"hbaseOperPutList(tableName: ${tableName})  count: ${seq.size}", "hbase", 0)
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
    * @param multiVersion
    * 是否get多版本
    * @param versions
    * get的版本数
    * @return
    * List[T]
    */
  def hbaseOperGetList[T <: HBaseBaseBean[T] : ClassTag](tableName: String, seq: Seq[Get], clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): Seq[T] = {
    this.mark
    val beanList = if (multiVersion) {
      seq.foreach(get => get.setMaxVersions(versions))
      HBaseOper.getMultiVersions(tableName, JavaConversions.seqAsJavaList(seq), clazz)
    } else {
      HBaseOper.get(tableName, JavaConversions.seqAsJavaList(seq), clazz)
    }
    this.logFire(s"hbaseOperGetList(tableName: ${tableName})  count: ${beanList.size}", "hbase", 0)
    JavaConversions.asScalaBuffer(beanList)
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
    * @param multiVersion
    * 是否get多版本
    * @param versions
    * get的版本数
    * @return
    * List[T]
    */
  def hbaseOperGetList2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, seq: Seq[String], clazz: Class[T], multiVersion: Boolean = false, versions: Int = Integer.MAX_VALUE): Seq[T] = {
    val getList = ListBuffer[Get]()
    seq.foreach(rowKey => {
      if (StringUtils.isNotBlank(rowKey)) {
        val get = new Get(rowKey.getBytes)
        if (multiVersion) {
          get.setMaxVersions(versions)
        }
        getList += get
      }
    })

    this.hbaseOperGetList[T](tableName, getList, clazz, multiVersion, versions)
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
    this.mark
    HBaseOper.deleteRow(tableName, JavaConversions.seqAsJavaList(rowKeys))
    this.logFire(s"hbaseOperDeleteList(tableName: ${tableName}) count: ${rowKeys.size}", "hbase", 0)
  }

  /**
    * 根据RDD[RowKey]批量删除记录
    *
    * @param tableName
    * hbase表名
    * @param rowKeyRDD
    * rowKey集合
    * @param batchSize
    * 一次删除多少条
    */
  def hbaseOperDeleteRDD(tableName: String, rowKeyRDD: RDD[String], batchSize: Int = this.batchSize): Unit = {
    this.mark
    rowKeyRDD.foreachPartition(it => {
      this.mark
      val rowKeyList = ListBuffer[String]()
      var count = 0
      it.foreach(rowKey => {
        if (StringUtils.isNotBlank(rowKey)) {
          rowKeyList += rowKey
          count += rowKeyList.size
        }
        if (rowKeyList.size >= batchSize) {
          HBaseOper.deleteRow(tableName, JavaConversions.seqAsJavaList(rowKeyList))
          rowKeyList.clear()
        }
      })
      if (rowKeyList.size > 0) {
        HBaseOper.deleteRow(tableName, JavaConversions.seqAsJavaList(rowKeyList))
        rowKeyList.clear()
      }
      this.logFire(s"hbaseOperDeleteRDD(tableName: ${tableName}) count: $count", "hbase", 0)
    })
    this.logFire(s"hbaseOperDeleteRDD(tableName: ${tableName})", "hbase", 0)
  }

  /**
    * 根据Dataset[RowKey]批量删除记录
    *
    * @param tableName
    * hbase表名
    * @param dataSet
    * rowKey集合
    * @param batchSize
    * 一次删除多少条
    */
  def hbaseOperDeleteDS(tableName: String, dataSet: Dataset[String], batchSize: Int = this.batchSize): Unit = {
    this.hbaseOperDeleteRDD(tableName, dataSet.rdd, batchSize)
  }

  /**
    * 按照指定的批次大小分多个批次插入数据到hbase中
    *
    * @param tableName
    * hbase表名
    * @param iterator
    * 数据集迭代器
    * @param insertEmpty
    * 为空的字段是否插入，默认为true
    * @param batchSize
    * 批次大小
    * @tparam E HBaseBaseBean的子类
    * @param multiVersion
    * 是否以多版本方式插入（会将多列数据转为一列的json数据进行保存）
    */
  private def multiBatchInsert[E <: HBaseBaseBean[E] : ClassTag](tableName: String, iterator: Iterator[E], insertEmpty: Boolean = true, batchSize: Int = this.batchSize, multiVersion: Boolean = false): Unit = {
    this.mark
    var count = 0
    val list = ListBuffer[E]()
    iterator.foreach(bean => {
      list += bean
      if (list.size >= batchSize) {
        if (multiVersion) {
          HBaseOper.insertMultiVersions(tableName, JavaConversions.seqAsJavaList(list))
        } else {
          if (insertEmpty) {
            HBaseOper.insert(tableName, list)
          } else {
            HBaseOper.insertIgnoreNull(tableName, list)
          }
        }
        count += list.size
        list.clear()
      }
    })
    if (list.size > 0) {
      if (multiVersion) {
        HBaseOper.insertMultiVersions(tableName, JavaConversions.seqAsJavaList(list))
      } else {
        if (insertEmpty) {
          HBaseOper.insert(tableName, list)
        } else {
          HBaseOper.insertIgnoreNull(tableName, list)
        }
      }
    }
    count += list.size
    list.clear()
    this.logFire(s"multiBatchInsert(tableName: ${tableName})  count: ${count}", "hbase", 0)
  }
}
