/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zto.fire.spark.ext.provider

import com.zto.fire.common.anno.API
import com.zto.fire._
import com.zto.fire.common.conf.KeyNum
import com.zto.fire.hbase.HBaseConnector
import com.zto.fire.hbase.bean.HBaseBaseBean
import com.zto.fire.hbase.conf.FireHBaseConf
import com.zto.fire.spark.connector.HBaseSparkBridge
import org.apache.hadoop.hbase.client.{Get, Scan}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
 * 为扩展层提供HBaseConnector相关API
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-12-23 17:39
 */
trait HBaseConnectorProvider extends SparkProvider {

  /**
   * Scan指定HBase表的数据，并映射为DataFrame
   *
   * @param tableName
   * HBase表名
   * @param scan
   * scan对象
   * @tparam T
   * 目标类型
   * @return
   */
  @API
  def hbaseScanDF[T <: HBaseBaseBean[T] : ClassTag](tableName: String, scan: Scan, keyNum: Int = KeyNum._1): DataFrame = {
    HBaseSparkBridge(keyNum = keyNum).hbaseScanDF[T](tableName, scan)
  }

  /**
   * Scan指定HBase表的数据，并映射为DataFrame
   *
   * @param tableName
   *                HBase表名
   * @param startRow
   *                开始主键
   * @param stopRow 结束主键
   * @tparam T
   * 目标类型
   * @return
   */
  @API
  def hbaseScanDF2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, startRow: String, stopRow: String, keyNum: Int = KeyNum._1): DataFrame = {
    HBaseSparkBridge(keyNum = keyNum).hbaseScanDF2[T](tableName, startRow, stopRow)
  }

  /**
   * Scan指定HBase表的数据，并映射为Dataset
   *
   * @param tableName
   * HBase表名
   * @param scan
   * scan对象
   * @tparam T
   * 目标类型
   * @return
   */
  @API
  def hbaseScanDS[T <: HBaseBaseBean[T] : ClassTag](tableName: String, scan: Scan, keyNum: Int = KeyNum._1): Dataset[T] = {
    HBaseSparkBridge(keyNum = keyNum).hbaseScanDS[T](tableName, scan)
  }

  /**
   * Scan指定HBase表的数据，并映射为Dataset
   *
   * @param tableName
   *                HBase表名
   * @param startRow
   *                开始主键
   * @param stopRow 结束主键
   * @tparam T
   * 目标类型
   * @return
   */
  @API
  def hbaseScanDS2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, startRow: String, stopRow: String, keyNum: Int = KeyNum._1): Dataset[T] = {
    HBaseSparkBridge(keyNum = keyNum).hbaseScanDS2[T](tableName, startRow, stopRow)
  }

  /**
   * 使用hbase java api方式插入一个集合的数据到hbase表中
   *
   * @param tableName
   * hbase表名
   * @param seq
   * HBaseBaseBean的子类集合
   */
  @API
  def hbasePutList[T <: HBaseBaseBean[T] : ClassTag](tableName: String, seq: Seq[T], keyNum: Int = KeyNum._1): Unit = {
    HBaseSparkBridge(keyNum = keyNum).hbasePutList[T](tableName, seq)
  }

  /**
   * 使用Java API的方式将RDD中的数据分多个批次插入到HBase中
   *
   * @param tableName
   * HBase表名
   */
  @API
  def hbasePutRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, rdd: RDD[T], keyNum: Int = KeyNum._1): Unit = {
    rdd.hbasePutRDD[T](tableName, keyNum)
  }

  /**
   * 使用Java API的方式将DataFrame中的数据分多个批次插入到HBase中
   *
   * @param tableName
   * HBase表名
   * @param df
   * DataFrame
   */
  @API
  def hbasePutDF[E <: HBaseBaseBean[E] : ClassTag](tableName: String, df: DataFrame, keyNum: Int = KeyNum._1): Unit = {
    df.hbasePutDF[E](tableName, keyNum)
  }

  /**
   * 使用Java API的方式将Dataset中的数据分多个批次插入到HBase中
   *
   * @param tableName
   * HBase表名
   */
  @API
  def hbasePutDS[E <: HBaseBaseBean[E] : ClassTag](tableName: String, dataset: Dataset[E], keyNum: Int = KeyNum._1): Unit = {
    dataset.hbasePutDS[E](tableName, keyNum)
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
  @API
  def hbaseScanRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, scan: Scan, keyNum: Int = KeyNum._1): RDD[T] = {
    HBaseSparkBridge(keyNum = keyNum).hbaseScanRDD[T](tableName, scan)
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
  @API
  def hbaseScanRDD2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, startRow: String, stopRow: String, keyNum: Int = KeyNum._1): RDD[T] = {
    HBaseSparkBridge(keyNum = keyNum).hbaseScanRDD[T](tableName, HBaseConnector.buildScan(startRow, stopRow))
  }

  /**
   * Scan指定HBase表的数据，并映射为List
   *
   * @param tableName
   * HBase表名
   * @param scan
   * hbase scan对象
   * @tparam T
   * 目标类型
   * @return
   */
  @API
  def hbaseScanList[T <: HBaseBaseBean[T] : ClassTag](tableName: String, scan: Scan, keyNum: Int = KeyNum._1): Seq[T] = {
    HBaseSparkBridge(keyNum = keyNum).hbaseScanList[T](tableName, scan)
  }

  /**
   * Scan指定HBase表的数据，并映射为List
   *
   * @param tableName
   *                HBase表名
   * @param startRow
   *                开始主键
   * @param stopRow 结束主键
   * @tparam T
   * 目标类型
   * @return
   */
  @API
  def hbaseScanList2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, startRow: String, stopRow: String, keyNum: Int = KeyNum._1): Seq[T] = {
    HBaseSparkBridge(keyNum = keyNum).hbaseScanList2[T](tableName, startRow, stopRow)
  }

  /**
   * 通过RDD[String]批量获取对应的数据（可获取历史版本的记录）
   *
   * @param tableName
   * HBase表名
   * @tparam T
   * 目标类型
   * @return
   */
  @API
  def hbaseGetRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, rdd: RDD[String], keyNum: Int = KeyNum._1): RDD[T] = {
    rdd.hbaseGetRDD[T](tableName, keyNum)
  }

  /**
   * 通过RDD[String]批量获取对应的数据（可获取历史版本的记录）
   *
   * @param tableName
   * HBase表名
   * @tparam T
   * 目标类型
   * @return
   */
  @API
  def hbaseGetDF[T <: HBaseBaseBean[T] : ClassTag](tableName: String, rdd: RDD[String], keyNum: Int = KeyNum._1): DataFrame = {
    rdd.hbaseGetDF[T](tableName, keyNum)
  }

  /**
   * 通过RDD[String]批量获取对应的数据（可获取历史版本的记录）
   *
   * @param tableName
   * HBase表名
   * @tparam T
   * 目标类型
   * @return
   */
  @API
  def hbaseGetDS[T <: HBaseBaseBean[T] : ClassTag](tableName: String, rdd: RDD[String], keyNum: Int = KeyNum._1): Dataset[T] = {
    rdd.hbaseGetDS[T](tableName, keyNum)
  }

  /**
   * 根据rowKey查询数据，并转为List[T]
   *
   * @param tableName
   * hbase表名
   * @param seq
   * rowKey集合
   * @return
   * List[T]
   */
  @API
  def hbaseGetList[T <: HBaseBaseBean[T] : ClassTag](tableName: String, seq: Seq[Get], keyNum: Int = KeyNum._1): Seq[T] = {
    HBaseSparkBridge(keyNum = keyNum).hbaseGetList[T](tableName, seq)
  }

  /**
   * 根据rowKey查询数据，并转为List[T]
   *
   * @param tableName
   * hbase表名
   * @param seq
   * rowKey集合
   * @return
   * List[T]
   */
  @API
  def hbaseGetList2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, seq: Seq[String], keyNum: Int = KeyNum._1): Seq[T] = {
    HBaseSparkBridge(keyNum = keyNum).hbaseGetList2[T](tableName, seq)
  }

  /**
   * 根据单个rowKey查询，并转为Map（单版本）
   * 无结果时返回空 Map
   */
  @API
  def hbaseGetMap(tableName: String, qualifiers: Seq[String], rowKey: String, keyNum: Int = KeyNum._1): Map[String, String] = {
    HBaseSparkBridge(keyNum = keyNum).hbaseGetMap(tableName, qualifiers, rowKey)
  }

  /**
   * 根据Get集合批量查询，并转为Map列表（单版本）
   */
  @API
  def hbaseGetMapList(tableName: String, qualifiers: Seq[String], seq: Seq[Get], keyNum: Int = KeyNum._1): ListBuffer[Map[String, String]] = {
    HBaseSparkBridge(keyNum = keyNum).hbaseGetMapList(tableName, qualifiers, seq)
  }

  /**
   * 根据rowKey集合批量查询，并转为Map列表（单版本）
   */
  @API
  def hbaseGetMapList2(tableName: String, qualifiers: Seq[String], seq: Seq[String], keyNum: Int = KeyNum._1): ListBuffer[Map[String, String]] = {
    HBaseSparkBridge(keyNum = keyNum).hbaseGetMapList2(tableName, qualifiers, seq)
  }

  /**
   * 多线程并发 GetMapList
   *
   * @param qualifiers 列限定符，支持 family:qualifier 或 qualifier（默认列族）；Nil 则拉整行
   */
  @API
  def hbaseGetMapListAsync(tableName: String, qualifiers: Seq[String], threadNum: Int, seq: Seq[Get], keyNum: Int = KeyNum._1): ListBuffer[Map[String, String]] = {
    HBaseSparkBridge(keyNum = keyNum).hbaseGetMapListAsync(tableName, qualifiers, threadNum, seq)
  }

  /**
   * 多线程并发 GetMapList（rowKey）
   *
   * @param qualifiers 列限定符，支持 family:qualifier 或 qualifier（默认列族）；Nil 则拉整行
   */
  @API
  def hbaseGetMapListAsync2(tableName: String, qualifiers: Seq[String], threadNum: Int, seq: Seq[String], keyNum: Int = KeyNum._1): ListBuffer[Map[String, String]] = {
    HBaseSparkBridge(keyNum = keyNum).hbaseGetMapListAsync2(tableName, qualifiers, threadNum, seq)
  }

  /**
   * Scan指定HBase表，并转为Map列表（单版本）
   */
  @API
  def hbaseScanMapList(tableName: String, qualifiers: Seq[String], scan: Scan, keyNum: Int = KeyNum._1): ListBuffer[Map[String, String]] = {
    HBaseSparkBridge(keyNum = keyNum).hbaseScanMapList(tableName, qualifiers, scan)
  }

  /**
   * 多线程并发 ScanMapList
   *
   * @param qualifiers 列限定符，支持 family:qualifier 或 qualifier（默认列族）；Nil 则拉整行
   */
  @API
  def hbaseScanMapListAsync(tableName: String, qualifiers: Seq[String], threadNum: Int, scan: Scan, keyNum: Int = KeyNum._1): ListBuffer[Map[String, String]] = {
    HBaseSparkBridge(keyNum = keyNum).hbaseScanMapListAsync(tableName, qualifiers, threadNum, scan)
  }

  /**
   * Scan指定HBase表（rowKey区间），并转为Map列表（单版本）
   */
  @API
  def hbaseScanMapList2(tableName: String, qualifiers: Seq[String], startRow: String, stopRow: String, keyNum: Int = KeyNum._1): ListBuffer[Map[String, String]] = {
    HBaseSparkBridge(keyNum = keyNum).hbaseScanMapList2(tableName, qualifiers, startRow, stopRow)
  }

  /**
   * 多线程并发 ScanMapList（rowKey 区间）
   *
   * @param qualifiers 列限定符，支持 family:qualifier 或 qualifier（默认列族）；Nil 则拉整行
   */
  @API
  def hbaseScanMapListAsync2(tableName: String, qualifiers: Seq[String], threadNum: Int, startRow: String, stopRow: String, keyNum: Int = KeyNum._1): ListBuffer[Map[String, String]] = {
    HBaseSparkBridge(keyNum = keyNum).hbaseScanMapListAsync2(tableName, qualifiers, threadNum, startRow, stopRow)
  }

  /**
   * 根据rowKey集合批量删除记录
   *
   * @param tableName
   * hbase表名
   * @param rowKeys
   * rowKey集合
   */
  @API
  def hbaseDeleteList(tableName: String, rowKeys: Seq[String], keyNum: Int = KeyNum._1): Unit = {
    HBaseSparkBridge(keyNum = keyNum).hbaseDeleteList(tableName, rowKeys)
  }

  /**
   * 根据RDD[RowKey]批量删除记录
   *
   * @param tableName
   * rowKey集合
   * @param rowKeyRDD
   * rowKey的rdd集合
   */
  @API
  def hbaseDeleteRDD(tableName: String, rowKeyRDD: RDD[String], keyNum: Int = KeyNum._1): Unit = {
    rowKeyRDD.hbaseDeleteRDD(tableName, keyNum)
  }

  /**
   * 根据Dataset[RowKey]批量删除记录
   *
   * @param tableName
   * rowKey集合
   */
  @API
  def hbaseDeleteDS(tableName: String, dataset: Dataset[String], keyNum: Int = KeyNum._1): Unit = {
    dataset.hbaseDeleteDS(tableName, keyNum)
  }

  /**
   * 多线程并发 Put 集合
   */
  @API
  def hbasePutListAsync[T <: HBaseBaseBean[T] : ClassTag](tableName: String, threadNum: Int, seq: Seq[T], keyNum: Int = KeyNum._1): Unit = {
    HBaseSparkBridge(keyNum = keyNum).hbasePutListAsync[T](tableName, threadNum, seq)
  }

  /**
   * 多线程并发 Put DataFrame
   */
  @API
  def hbasePutDFAsync[E <: HBaseBaseBean[E] : ClassTag](tableName: String, threadNum: Int, df: DataFrame, keyNum: Int = KeyNum._1): Unit = {
    HBaseSparkBridge(keyNum = keyNum).hbasePutDFAsync[E](tableName, threadNum, df)
  }

  /**
   * 多线程并发 Put Dataset
   */
  @API
  def hbasePutDSAsync[E <: HBaseBaseBean[E] : ClassTag](tableName: String, threadNum: Int, dataset: Dataset[E], keyNum: Int = KeyNum._1): Unit = {
    HBaseSparkBridge(keyNum = keyNum).hbasePutDSAsync[E](tableName, threadNum, dataset)
  }

  /**
   * 多线程并发 Put RDD
   */
  @API
  def hbasePutRDDAsync[T <: HBaseBaseBean[T] : ClassTag](tableName: String, threadNum: Int, rdd: RDD[T], keyNum: Int = KeyNum._1): Unit = {
    HBaseSparkBridge(keyNum = keyNum).hbasePutRDDAsync[T](tableName, threadNum, rdd)
  }

  /**
   * 多线程并发 Get
   */
  @API
  def hbaseGetListAsync[T <: HBaseBaseBean[T] : ClassTag](tableName: String, threadNum: Int, seq: Seq[Get], keyNum: Int = KeyNum._1): Seq[T] = {
    HBaseSparkBridge(keyNum = keyNum).hbaseGetListAsync[T](tableName, threadNum, seq)
  }

  /**
   * 多线程并发 Get（rowKey）
   */
  @API
  def hbaseGetListAsync2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, threadNum: Int, seq: Seq[String], keyNum: Int = KeyNum._1): Seq[T] = {
    HBaseSparkBridge(keyNum = keyNum).hbaseGetListAsync2[T](tableName, threadNum, seq)
  }

  /**
   * 多线程并发 Scan
   */
  @API
  def hbaseScanListAsync[T <: HBaseBaseBean[T] : ClassTag](tableName: String, threadNum: Int, scan: Scan, keyNum: Int = KeyNum._1): Seq[T] = {
    HBaseSparkBridge(keyNum = keyNum).hbaseScanListAsync[T](tableName, threadNum, scan)
  }

  /**
   * 多线程并发 Scan（rowKey 区间）
   */
  @API
  def hbaseScanListAsync2[T <: HBaseBaseBean[T] : ClassTag](tableName: String, threadNum: Int, startRow: String, stopRow: String, keyNum: Int = KeyNum._1): Seq[T] = {
    HBaseSparkBridge(keyNum = keyNum).hbaseScanListAsync2[T](tableName, threadNum, startRow, stopRow)
  }
}
