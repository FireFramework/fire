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

package com.zto.fire.flink.ext.provider

import com.zto.fire.common.anno.API
import com.zto.fire._
import com.zto.fire.common.conf.KeyNum
import com.zto.fire.hbase.HBaseConnector
import com.zto.fire.hbase.bean.HBaseBaseBean
import com.zto.fire.hbase.conf.FireHBaseConf
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.Table
import org.apache.flink.types.Row
import org.apache.hadoop.hbase.client.{Get, Scan}

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
 * 为上层扩展层提供HBaseConnector API
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2020-12-24 10:16
 */
trait HBaseConnectorProvider {

  /**
   * hbase批量sink操作，DataStream[T]中的T必须是HBaseBaseBean的子类
   *
   * @param tableName
   * hbase表名
   * @param batch
   * 每次sink最大的记录数
   * @param flushInterval
   * 多久flush一次（毫秒）
   * @param keyNum
   * 配置文件中的key后缀
   */
  @API
  def hbasePutDS[T <: HBaseBaseBean[T]: ClassTag](stream: DataStream[T],
                                        tableName: String,
                                        batch: Int = 100,
                                        flushInterval: Long = 3000,
                                        keyNum: Int = KeyNum._1): DataStreamSink[_] = {
    stream.hbasePutDS(tableName, batch, flushInterval, keyNum)
  }

  /**
   * hbase批量sink操作，DataStream[T]中的T必须是HBaseBaseBean的子类
   *
   * @param tableName
   * hbase表名
   * @param batch
   * 每次sink最大的记录数
   * @param flushInterval
   * 多久flush一次（毫秒）
   * @param keyNum
   * 配置文件中的key后缀
   * @param fun
   * 将dstream中的数据映射为该sink组件所能处理的数据
   */
  @API
  def hbasePutDS2[T <: HBaseBaseBean[T] : ClassTag](stream: DataStream[T],
                                                    tableName: String,
                                                    batch: Int = 100,
                                                    flushInterval: Long = 3000,
                                                    keyNum: Int = KeyNum._1)(fun: T => T): DataStreamSink[_] = {
    stream.hbasePutDS2[T](tableName, batch, flushInterval, keyNum)(fun)
  }

  /**
   * table的hbase批量sink操作，该api需用户定义row的取数规则，并映射到对应的HBaseBaseBean的子类中
   *
   * @param tableName
   *                     HBase表名
   * @param batch
   *                     每次sink最大的记录数
   * @param flushInterval
   *                     多久flush一次（毫秒）
   * @param keyNum
   *                     配置文件中的key后缀
   */
  @API
  def hbasePutTable[T <: HBaseBaseBean[T]: ClassTag](table: Table,
                                           tableName: String,
                                           batch: Int = 100,
                                           flushInterval: Long = 3000,
                                           keyNum: Int = KeyNum._1): DataStreamSink[_] = {
    table.hbasePutTable[T](tableName, batch, flushInterval, keyNum)
  }

  /**
   * table的hbase批量sink操作，该api需用户定义row的取数规则，并映射到对应的HBaseBaseBean的子类中
   *
   * @param tableName
   *                     HBase表名
   * @param batch
   *                     每次sink最大的记录数
   * @param flushInterval
   *                     多久flush一次（毫秒）
   * @param keyNum
   *                     配置文件中的key后缀
   */
  @API
  def hbasePutTable2[T <: HBaseBaseBean[T]: ClassTag](table: Table,
                                           tableName: String,
                                           batch: Int = 100,
                                           flushInterval: Long = 3000,
                                           keyNum: Int = KeyNum._1)(fun: Row => T): DataStreamSink[_] = {
    table.hbasePutTable2[T](tableName, batch, flushInterval, keyNum)(fun)
  }

  /**
   * stream hbase sink（多线程并发写入）
   */
  @API
  def hbasePutDSAsync[T <: HBaseBaseBean[T]: ClassTag](stream: DataStream[T],
                                                       tableName: String,
                                                       batch: Int = 100,
                                                       flushInterval: Long = 3000,
                                                       threadNum: Int = FireHBaseConf.hbaseThreadNum(),
                                                       keyNum: Int = KeyNum._1): DataStreamSink[_] = {
    stream.hbasePutDSAsync(tableName, batch, flushInterval, threadNum = threadNum, keyNum = keyNum)
  }

  /**
   * stream hbase sink（多线程并发写入）
   */
  @API
  def hbasePutDSAsync2[T <: HBaseBaseBean[T] : ClassTag](stream: DataStream[T],
                                                         tableName: String,
                                                         batch: Int = 100,
                                                         flushInterval: Long = 3000,
                                                         threadNum: Int = FireHBaseConf.hbaseThreadNum(),
                                                         keyNum: Int = KeyNum._1)(fun: T => T): DataStreamSink[_] = {
    stream.hbasePutDSAsync2[T](tableName, batch, flushInterval, threadNum = threadNum, keyNum = keyNum)(fun)
  }

  /**
   * table hbase sink（多线程并发写入）
   */
  @API
  def hbasePutTableAsync[T <: HBaseBaseBean[T]: ClassTag](table: Table,
                                                          tableName: String,
                                                          batch: Int = 100,
                                                          flushInterval: Long = 3000,
                                                          threadNum: Int = FireHBaseConf.hbaseThreadNum(),
                                                          keyNum: Int = KeyNum._1): DataStreamSink[_] = {
    table.hbasePutTableAsync[T](tableName, batch, flushInterval, threadNum = threadNum, keyNum = keyNum)
  }

  /**
   * table hbase sink（多线程并发写入）
   */
  @API
  def hbasePutTableAsync2[T <: HBaseBaseBean[T]: ClassTag](table: Table,
                                                           tableName: String,
                                                           batch: Int = 100,
                                                           flushInterval: Long = 3000,
                                                           threadNum: Int = FireHBaseConf.hbaseThreadNum(),
                                                           keyNum: Int = KeyNum._1)(fun: Row => T): DataStreamSink[_] = {
    table.hbasePutTableAsync2[T](tableName, batch, flushInterval, threadNum = threadNum, keyNum = keyNum)(fun)
  }

  /**
   * 多线程并发 Get
   */
  @API
  def hbaseGetListAsync[T <: HBaseBaseBean[T]: ClassTag](tableName: String, threadNum: Int, gets: Seq[Get],
                                                         keyNum: Int = KeyNum._1): Seq[T] = {
    HBaseConnector.getAsync[T](tableName, threadNum, ListBuffer(gets: _*), keyNum)
  }

  /**
   * 多线程并发 Get（rowKey）
   */
  @API
  def hbaseGetListAsync2[T <: HBaseBaseBean[T]: ClassTag](tableName: String, threadNum: Int, rowKeys: Seq[String],
                                                          keyNum: Int = KeyNum._1): Seq[T] = {
    HBaseConnector.getAsync[T](tableName, threadNum, rowKeys, keyNum)
  }

  /**
   * 多线程并发 Scan
   */
  @API
  def hbaseScanListAsync[T <: HBaseBaseBean[T]: ClassTag](tableName: String, threadNum: Int, scan: org.apache.hadoop.hbase.client.Scan,
                                                          keyNum: Int = KeyNum._1): Seq[T] = {
    HBaseConnector.scanAsync[T](tableName, threadNum, scan, keyNum)
  }

  /**
   * 多线程并发 Scan（rowKey 区间）
   */
  @API
  def hbaseScanListAsync2[T <: HBaseBaseBean[T]: ClassTag](tableName: String, threadNum: Int, startRow: String, stopRow: String,
                                                           keyNum: Int = KeyNum._1): Seq[T] = {
    HBaseConnector.scanAsync[T](tableName, threadNum, startRow, stopRow, keyNum)
  }

  /**
   * 多线程并发 Put 集合
   */
  @API
  def hbasePutListAsync[T <: HBaseBaseBean[T]: ClassTag](tableName: String, threadNum: Int, seq: Seq[T], keyNum: Int = KeyNum._1): Unit = {
    HBaseConnector.insertAsync[T](tableName, threadNum, seq, keyNum)
  }

  /**
   * 根据单个rowKey查询，并转为Map（单版本）
   * 无结果时返回空 Map
   */
  @API
  def hbaseGetMap(tableName: String, qualifiers: Seq[String], rowKey: String, keyNum: Int = KeyNum._1): Map[String, String] = {
    HBaseConnector(keyNum = keyNum).getMap(tableName, qualifiers, rowKey)
  }

  /**
   * 根据Get集合批量查询，并转为Map列表（单版本）
   */
  @API
  def hbaseGetMapList(tableName: String, qualifiers: Seq[String], seq: Seq[Get], keyNum: Int = KeyNum._1): ListBuffer[Map[String, String]] = {
    HBaseConnector(keyNum = keyNum).getMapList(tableName, qualifiers, seq: _*)
  }

  /**
   * 根据rowKey集合批量查询，并转为Map列表（单版本）
   */
  @API
  def hbaseGetMapList2(tableName: String, qualifiers: Seq[String], seq: Seq[String], keyNum: Int = KeyNum._1): ListBuffer[Map[String, String]] = {
    HBaseConnector(keyNum = keyNum).getMapList(tableName, qualifiers, seq: _*)
  }

  /**
   * 多线程并发 GetMapList
   *
   * @param qualifiers 列限定符，支持 family:qualifier 或 qualifier（默认列族）；Nil 则拉整行
   */
  @API
  def hbaseGetMapListAsync(tableName: String, qualifiers: Seq[String], threadNum: Int, gets: Seq[Get], keyNum: Int = KeyNum._1): ListBuffer[Map[String, String]] = {
    HBaseConnector.getMapListAsync(tableName, qualifiers, threadNum, ListBuffer(gets: _*), keyNum)
  }

  /**
   * 多线程并发 GetMapList（rowKey）
   *
   * @param qualifiers 列限定符，支持 family:qualifier 或 qualifier（默认列族）；Nil 则拉整行
   */
  @API
  def hbaseGetMapListAsync2(tableName: String, qualifiers: Seq[String], threadNum: Int, rowKeys: Seq[String], keyNum: Int = KeyNum._1): ListBuffer[Map[String, String]] = {
    HBaseConnector.getMapListAsync(tableName, qualifiers, threadNum, rowKeys, keyNum)
  }

  /**
   * Scan指定HBase表，并转为Map列表（单版本）
   */
  @API
  def hbaseScanMapList(tableName: String, qualifiers: Seq[String], scan: Scan, keyNum: Int = KeyNum._1): ListBuffer[Map[String, String]] = {
    HBaseConnector(keyNum = keyNum).scanMapList(tableName, qualifiers, scan)
  }

  /**
   * 多线程并发 ScanMapList
   *
   * @param qualifiers 列限定符，支持 family:qualifier 或 qualifier（默认列族）；Nil 则拉整行
   */
  @API
  def hbaseScanMapListAsync(tableName: String, qualifiers: Seq[String], threadNum: Int, scan: Scan, keyNum: Int = KeyNum._1): ListBuffer[Map[String, String]] = {
    HBaseConnector.scanMapListAsync(tableName, qualifiers, threadNum, scan, keyNum)
  }

  /**
   * Scan指定HBase表（rowKey区间），并转为Map列表（单版本）
   */
  @API
  def hbaseScanMapList2(tableName: String, qualifiers: Seq[String], startRow: String, stopRow: String, keyNum: Int = KeyNum._1): ListBuffer[Map[String, String]] = {
    HBaseConnector(keyNum = keyNum).scanMapList(tableName, qualifiers, startRow, stopRow)
  }

  /**
   * 多线程并发 ScanMapList（rowKey 区间）
   *
   * @param qualifiers 列限定符，支持 family:qualifier 或 qualifier（默认列族）；Nil 则拉整行
   */
  @API
  def hbaseScanMapListAsync2(tableName: String, qualifiers: Seq[String], threadNum: Int, startRow: String, stopRow: String, keyNum: Int = KeyNum._1): ListBuffer[Map[String, String]] = {
    HBaseConnector.scanMapListAsync(tableName, qualifiers, threadNum, startRow, stopRow, keyNum)
  }

}
