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

package com.zto.fire.hbase

import com.google.common.collect.Maps
import com.zto.fire.common.anno.{FieldName, Internal}
import com.zto.fire.common.conf.{FireFrameworkConf, FireHDFSConf, KeyNum}
import com.zto.fire.common.enu.Datasource.HBASE
import com.zto.fire.common.enu.{Operation => FOperation}
import com.zto.fire.common.lineage.parser.connector.HbaseConnectorParser
import com.zto.fire.common.util._
import com.zto.fire.core.connector.{ConnectorFactory, FireConnector}
import com.zto.fire.hbase.HBaseConnector.addHBaseDatasource
import com.zto.fire.hbase.anno.HConfig
import com.zto.fire.hbase.bean.{HBaseBaseBean, MultiVersionsBean}
import com.zto.fire.hbase.conf.FireHBaseConf
import com.zto.fire.hbase.utils.HBaseUtils
import com.zto.fire.hbase.conf.FireHBaseConf
import com.zto.fire.hbase.conf.FireHBaseConf._
import com.zto.fire.predef._
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.security.User
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.security.UserGroupInformation

import java.lang.reflect.Field
import java.lang.{Boolean => JBoolean, Double => JDouble, Float => JFloat, Integer => JInt, Long => JLong, Short => JShort, String => JString}
import java.math.{BigDecimal => JBigDecimal}
import java.nio.charset.{CharacterCodingException, StandardCharsets}
import java.util.concurrent.{TimeUnit, ConcurrentHashMap => JConcurrentHashMap}
import java.util.{Map => JMap}
import scala.collection.mutable.ListBuffer
import scala.reflect.{ClassTag, classTag}

/**
 * HBase操作工具类，除了涵盖CRUD等常用操作外，还提供以下功能：
 * 1. static <T extends HBaseBaseBean> void insert(String tableName, String family, List<T> list)
 * 将自定义的javabean集合批量插入到表中
 * 2. scan[T <: HBaseBaseBean[T]](tableName: String, scan: Scan, clazz: Class[T], keyNum: Int = 1): ListBuffer[T]
 * 指定查询条件，将查询结果以List[T]形式返回
 * 注：自定义bean中的field需与hbase中的qualifier对应
 * <p>
 *
 * @param conf
 * 代码级别的配置信息，允许为空，配置文件会覆盖相同配置项，也就是说配置文件拥有着跟高的优先级
 * @param keyNum
 * 用于区分连接不同的数据源，不同配置源对应不同的Connector实例
 * @since 2.0.0
 * @author ChengLong 2020-11-11
 */
class HBaseConnector(val conf: Configuration = null, val keyNum: Int = KeyNum._1) extends FireConnector(keyNum = keyNum) {
  // --------------------------------------- 反射缓存 --------------------------------------- //
  private[this] var configuration: Configuration = _
  private[this] lazy val cacheFieldMap = new JConcurrentHashMap[Class[_], JMap[String, Field]]()
  // Bean 可查询列 (family, qualifier) 字节缓存，供内部 buildGet/buildScan 投影使用
  private[this] lazy val cacheQueryableColumns = new JConcurrentHashMap[Class[_], Array[(Array[Byte], Array[Byte])]]()
  private[this] lazy val cacheHConfigMap = new JConcurrentHashMap[Class[_], HConfig]()
  private[this] lazy val cacheTableExistsMap = new JConcurrentHashMap[String, Boolean]()
  private[this] lazy val connection: Connection = this.initConnection
  private[this] lazy val durability = this.initDurability
  // ------------------------------------ 表存在判断缓存 ------------------------------------ //
  private[this] lazy val tableExistsCacheEnable = tableExistsCache(this.keyNum)
  private[this] lazy val closeAdminError = "close admin执行失败"
  this.registerReload

  /**
   * 批量插入多行多列，自动将HBaseBaseBean子类转为Put集合
   *
   * @param tableName 表名
   * @param beans     HBaseBaseBean子类集合
   */
  def insert[T <: HBaseBaseBean[T] : ClassTag](tableName: String, beans: T*): Unit = {
    checkGeneric[T]("HBaseConnector.insert")
    requireNonNull(tableName, beans)("参数不合法，批量HBase insert失败")

    var table: Table = null
    tryFinallyWithReturn {
      table = this.getTable(tableName)
      val beanList = if (this.getMultiVersion[T]) beans.filter(_ != null).map((bean: T) => new MultiVersionsBean(bean)) else beans
      val putList = beanList.map(bean => convert2Put[T](bean.asInstanceOf[T], this.getNullable[T]))
      this.insert(tableName, putList: _*)
    } {
      this.closeTable(table)
    }(this.logger, catchLog = s"HBase insert ${hbaseClusterUrl(keyNum)}.${tableName}执行失败, 总计${beans.size}条", finallyCatchLog = "close HBase table失败")
  }

  /**
   * 批量插入多行多列
   *
   * @param tableName 表名
   * @param puts      Put集合
   */
  def insert(tableName: String, puts: Put*): Unit = {
    requireNonNull(tableName, puts)("参数不合法，批量HBase insert失败")

    var table: Table = null
    tryFinallyWithReturn {
      table = this.getTable(tableName)
      table.put(puts)
      addHBaseDatasource(tableName, FOperation.INSERT, keyNum)
      logInfo(s"HBase insert ${hbaseClusterUrl(keyNum)}.${tableName}执行成功, 总计${puts.size}条")
    } {
      this.closeTable(table)
    }(this.logger, "HBase insert",
      s"HBase insert ${hbaseClusterUrl(keyNum)}.${tableName}执行失败, 总计${puts.size}条",
      "close HBase table失败")
  }

  /**
   * 多线程并发 Put，底层共享 HBase Connection，每个分组独立获取 Table 执行写入
   *
   * 注意：
   * 1. `threadNum=1` 时等价于单线程 `insert`
   * 2. 同 rowKey 并发写入不保证顺序
   * 3. HBase Connection 线程安全，无需独立连接池
   */
  def insertAsync(tableName: String, threadNum: Int, puts: Put*): Unit = {
    require(threadNum > 0, s"线程数量必须大于0，当前值：$threadNum")
    if (puts == null || puts.isEmpty) return
    if (threadNum <= 1 || puts.size <= 1) {
      this.insert(tableName, puts: _*)
      return
    }

    val parallelism = math.min(threadNum, puts.size)
    ThreadUtils.parallelProcess(puts, parallelism) { partition =>
      this.insert(tableName, partition: _*)
    }
  }

  /**
   * 多线程并发 Put（Bean 映射），底层共享 HBase Connection
   */
  def insertAsync[T <: HBaseBaseBean[T] : ClassTag](tableName: String, threadNum: Int, beans: Seq[T]): Unit = {
    require(threadNum > 0, s"线程数量必须大于0，当前值：$threadNum")
    if (beans == null || beans.isEmpty) return
    if (threadNum <= 1 || beans.size <= 1) {
      this.insert[T](tableName, beans: _*)
      return
    }
    val parallelism = math.min(threadNum, beans.size)
    ThreadUtils.parallelProcess(beans, parallelism) { partition =>
      this.insert[T](tableName, partition: _*)
    }
  }

  /**
   * 从HBase批量Get数据，并将结果封装到JavaBean中
   *
   * @param tableName 表名
   * @param rowKeys   指定的多个rowKey
   * @return 目标对象实例
   */
  def get[T <: HBaseBaseBean[T] : ClassTag](tableName: String, rowKeys: String*): ListBuffer[T] = {
    val getList = for (rowKey <- rowKeys) yield this.buildGet[T](rowKey)
    this.get[T](tableName, getList: _*)
  }

  /**
   * 从HBase批量Get数据，并将结果封装到JavaBean中
   *
   * @param tableName 表名
   * @param gets      指定的多个get对象
   * @return 目标对象实例
   */
  def get[T <: HBaseBaseBean[T] : ClassTag](tableName: String, gets: Get*)(implicit canOverload: Boolean = true): ListBuffer[T] = {
    val clazz = getGeneric[T]("HBaseConnector.get")
    requireNonNull(tableName, clazz, gets)("参数不合法，无法进行HBase Get操作")

    tryWithReturn {
      gets.foreach(get => this.applyBeanColumns[T](get))
      this.getMaxVersions[T](gets: _*)
      val resultList = this.getResult(tableName, gets: _*)
      if (this.getMultiVersion[T]) this.hbaseMultiRow2Bean[T](resultList, clazz) else this.hbaseRow2Bean[T](resultList)
    }(this.logger, catchLog = s"批量 get ${hbaseClusterUrl(keyNum)}.${tableName}执行失败")
  }

  /**
   * 多线程并发 Get，底层共享 HBase Connection（默认线程数）
   */
  def getAsync[T <: HBaseBaseBean[T] : ClassTag](tableName: String, gets: Get*)(implicit canOverload: Boolean = true): ListBuffer[T] = {
    this.getAsync[T](tableName, FireHBaseConf.hbaseThreadNum(this.keyNum), gets: _*)
  }

  /**
   * 多线程并发 Get，底层共享 HBase Connection
   */
  def getAsync[T <: HBaseBaseBean[T] : ClassTag](tableName: String, threadNum: Int, gets: Get*)(implicit canOverload: Boolean): ListBuffer[T] = {
    require(threadNum > 0, s"线程数量必须大于0，当前值：$threadNum")
    if (gets == null || gets.isEmpty) return ListBuffer.empty[T]
    if (threadNum <= 1 || gets.size <= 1) return this.get[T](tableName, gets: _*)

    val parallelism = math.min(threadNum, gets.size)
    val result = ListBuffer[T]()
    ThreadUtils.parallelProcess(gets, parallelism) { partition =>
      this.get[T](tableName, partition: _*)
    }.foreach(result ++= _)
    result
  }

  /**
   * 多线程并发 Get（rowKey），底层共享 HBase Connection
   */
  def getAsync[T <: HBaseBaseBean[T] : ClassTag](tableName: String, threadNum: Int, rowKeys: String*): ListBuffer[T] = {
    if (rowKeys == null || rowKeys.isEmpty) return ListBuffer.empty[T]
    implicit def canOverload: Boolean = true
    this.getAsync[T](tableName, threadNum, rowKeys.map(rowKey => HBaseConnector.buildGet(rowKey)): _*)
  }

  /**
   * 从HBase Get单条数据，并将结果封装为Map（单版本）
   * Map结构：rowKey + family:qualifier -> value（启发式还原后转为 String）
   * 无记录时返回 null
   *
   * @param tableName   表名
   * @param qualifiers  列限定符，支持 family:qualifier 或 qualifier（默认列族）；Nil 则拉整行
   * @param rowKey      指定的rowKey
   * @return Map结果
   */
  def getMap(tableName: String, qualifiers: Seq[String], rowKey: String): Map[String, String] = {
    requireNonEmpty(tableName, rowKey)("参数不合法，无法进行HBase getMap操作")
    val maps = this.getMapList(tableName, qualifiers, rowKey)
    if (maps == null || maps.isEmpty) Map.empty else maps.head
  }

  /**
   * 从HBase批量Get数据，并将结果封装为Map列表（单版本）
   * Map结构：rowKey + family:qualifier -> value（启发式还原后转为 String）
   *
   * @param tableName   表名
   * @param qualifiers  列限定符，支持 family:qualifier 或 qualifier（默认列族）；Nil 则拉整行
   * @param rowKeys     指定的多个rowKey
   * @return Map结果集
   */
  def getMapList(tableName: String, qualifiers: Seq[String], rowKeys: String*): ListBuffer[Map[String, String]] = {
    val getList = for (rowKey <- rowKeys) yield HBaseConnector.buildGet(rowKey)
    this.getMapList(tableName, qualifiers, getList: _*)
  }

  /**
   * 从HBase批量Get数据，并将结果封装为Map列表（单版本）
   * Map结构：rowKey + family:qualifier -> value（启发式还原后转为 String）
   *
   * @param tableName   表名
   * @param qualifiers  列限定符，支持 family:qualifier 或 qualifier（默认列族）；Nil 则拉整行
   * @param gets        指定的多个get对象
   * @return Map结果集
   */
  def getMapList(tableName: String, qualifiers: Seq[String], gets: Get*)(implicit canOverload: Boolean = true): ListBuffer[Map[String, String]] = {
    requireNonNull(tableName, gets)("参数不合法，无法进行HBase getMapList操作")
    gets.foreach(get => this.applyMapQualifiers(get, qualifiers: _*))
    tryWithReturn {
      val resultList = this.getResult(tableName, gets: _*)
      this.hbaseRow2Map(resultList)
    }(this.logger, s"HBase getMapList ${hbaseClusterUrl(keyNum)}.${tableName}",
      s"批量 getMapList ${hbaseClusterUrl(keyNum)}.${tableName}执行失败")
  }

  /**
   * 多线程并发 GetMapList，底层共享 HBase Connection（默认线程数）
   */
  def getMapListAsync(tableName: String, gets: Get*)(implicit canOverload: Boolean = true): ListBuffer[Map[String, String]] = {
    this.getMapListAsync(tableName, FireHBaseConf.hbaseThreadNum(this.keyNum), gets: _*)
  }

  /**
   * 多线程并发 GetMapList，底层共享 HBase Connection
   */
  def getMapListAsync(tableName: String, threadNum: Int, gets: Get*)(implicit canOverload: Boolean): ListBuffer[Map[String, String]] = {
    require(threadNum > 0, s"线程数量必须大于0，当前值：$threadNum")
    if (gets == null || gets.isEmpty) return ListBuffer.empty[Map[String, String]]
    if (threadNum <= 1 || gets.size <= 1) return this.getMapList(tableName, gets: _*)

    val parallelism = math.min(threadNum, gets.size)
    val result = ListBuffer[Map[String, String]]()
    ThreadUtils.parallelProcess(gets, parallelism) { partition =>
      this.getMapList(tableName, partition: _*)
    }.foreach(result ++= _)
    result
  }

  /**
   * 多线程并发 GetMapList（rowKey），底层共享 HBase Connection
   */
  def getMapListAsync(tableName: String, threadNum: Int, rowKeys: String*): ListBuffer[Map[String, String]] = {
    if (rowKeys == null || rowKeys.isEmpty) return ListBuffer.empty[Map[String, String]]
    implicit def canOverload: Boolean = true
    this.getMapListAsync(tableName, threadNum, rowKeys.map(rowKey => HBaseConnector.buildGet(rowKey)): _*)
  }

  /**
   * 通过HBase Seq[Get]获取多条数据
   *
   * @param tableName 表名
   * @param getList   HBase的get对象实例
   * @return
   * HBase Result
   */
  def getResult(tableName: String, getList: Get*): ListBuffer[Result] = {
    requireNonNull(tableName, getList)("参数不合法，执行HBase 批量get失败")

    var table: Table = null
    val list = ListBuffer[Result]()
    tryFinallyWithReturn {
      addHBaseDatasource(tableName, FOperation.GET, keyNum)
      table = this.getTable(tableName)
      list ++= table.get(getList)
      logInfo(s"HBase 批量get ${hbaseClusterUrl(keyNum)}.${tableName}执行成功, 总计${list.size}条")
      list
    } {
      this.closeTable(table)
    }(this.logger, "HBase get",
      s"get ${hbaseClusterUrl(keyNum)}.${tableName}执行失败", "close HBase table对象失败.")
  }

  /**
   * 通过HBase Get对象获取一条数据
   *
   * @param tableName 表名
   * @return
   * HBase Result
   */
  def getResult[T: ClassTag](tableName: String, rowKeyList: String*): ListBuffer[Result] = {
    requireNonNull(tableName, rowKeyList)("参数不合法，rowKey集合不能为空.")
    val getList = for (rowKey <- rowKeyList) yield HBaseConnector.buildGet(rowKey)
    val starTime = currentTime
    val resultList = this.getResult(tableName, getList: _*)
    logInfo(s"HBase 批量get ${hbaseClusterUrl(keyNum)}.${tableName}执行成功, 总计${resultList.size}条, 耗时：${elapsed(starTime)}")
    resultList
  }

  /**
   * 表扫描，将scan后得到的ResultScanner对象直接返回
   * 注：调用者需手动关闭ResultScanner对象实例
   *
   * @param tableName 表名
   * @param scan      HBase scan对象
   * @return 指定类型的List
   */
  def scanResultScanner(tableName: String, scan: Scan): ResultScanner = {
    requireNonEmpty(tableName, scan)(s"参数不合法，scan ${hbaseClusterUrl(keyNum)}.${tableName}失败.")

    var table: Table = null
    var rsScanner: ResultScanner = null
    try {
      table = this.getTable(tableName)
      addHBaseDatasource(tableName, FOperation.SCAN, keyNum)
      rsScanner = table.getScanner(scan)
    } catch {
      case e: Exception => {
        // 当执行scan失败时，向上抛异常之前，避免ResultScanner对象因异常无法得到有效的关闭
        // 因此在发生异常时会尝试关闭ResultScanner对象
        logError(s"执行scan ${hbaseClusterUrl(keyNum)}.${tableName}失败", e)
        try {
          this.closeResultScanner(rsScanner)
        } finally {
          throw e
        }
      }
    } finally {
      this.closeTable(table)
    }

    rsScanner
  }

  /**
   * 表扫描，将scan后得到的ResultScanner对象直接返回
   * 注：调用者需手动关闭ResultScanner对象实例
   *
   * @param tableName 表名
   * @param startRow  开始行
   * @param endRow    结束行
   * @return 指定类型的List
   */
  def scanResultScanner(tableName: String, startRow: String, endRow: String): ResultScanner = {
    requireNonEmpty(tableName, startRow, endRow)
    val scan = HBaseConnector.buildScan(startRow, endRow)
    this.scanResultScanner(tableName, scan)
  }

  /**
   * 表扫描，将查询后的数据转为JavaBean并放到List中
   *
   * @param tableName 表名
   * @param startRow  开始行
   * @param endRow    结束行
   * @return 指定类型的List
   */
  def scan[T <: HBaseBaseBean[T] : ClassTag](tableName: String, startRow: String, endRow: String): ListBuffer[T] = {
    requireNonEmpty(tableName, startRow, endRow)
    val scan = this.buildScan[T](startRow, endRow)
    this.scan[T](tableName, scan)
  }

  /**
   * 表扫描，将查询后的数据转为JavaBean并放到List中
   *
   * @param tableName 表名
   * @param scan      HBase scan对象
   * @return 指定类型的List
   */
  def scan[T <: HBaseBaseBean[T] : ClassTag](tableName: String, scan: Scan): ListBuffer[T] = {
    checkGeneric[T]("HBaseConnector.scan")
    requireNonEmpty(tableName, scan)(s"参数不合法，scan ${hbaseClusterUrl(keyNum)}.${tableName}失败.")

    val list = ListBuffer[T]()
    var rsScanner: ResultScanner = null
    tryFinallyWithReturn {
      this.applyBeanColumns[T](scan)
      this.setScanMaxVersions[T](scan)
      rsScanner = this.scanResultScanner(tableName, scan)
      if (rsScanner != null) {
        rsScanner.foreach(rs => {
          if (this.getMultiVersion[T]) {
            val objList = this.hbaseMultiRow2Bean[T](rs)
            if (objList != null && objList.nonEmpty) list ++= objList
          } else {
            val obj = hbaseRow2Bean[T](rs)
            if (obj.isDefined) list += obj.get
          }
        })
      }
      logInfo(s"HBase scan ${hbaseClusterUrl(keyNum)}.${tableName}执行成功, 总计${list.size}条")
      list
    } {
      this.closeResultScanner(rsScanner)
    }(this.logger, "HBase scan",
      s"scan ${hbaseClusterUrl(keyNum)}.${tableName}执行失败",
      "关闭HBase table对象或ResultScanner失败")
  }

  /**
   * 表扫描，将查询后的数据转为Map列表（单版本）
   * Map结构：rowKey + family:qualifier -> value（启发式还原后转为 String）
   *
   * @param tableName   表名
   * @param qualifiers  列限定符，支持 family:qualifier 或 qualifier（默认列族）；Nil 则拉整行
   * @param startRow    开始行
   * @param endRow      结束行
   * @return Map结果集
   */
  def scanMapList(tableName: String, qualifiers: Seq[String], startRow: String, endRow: String): ListBuffer[Map[String, String]] = {
    requireNonEmpty(tableName, startRow, endRow)
    val scan = HBaseConnector.buildScan(startRow, endRow)
    this.scanMapList(tableName, qualifiers, scan)
  }

  /**
   * 表扫描，将查询后的数据转为Map列表（单版本）
   * Map结构：rowKey + family:qualifier -> value（启发式还原后转为 String）
   *
   * @param tableName   表名
   * @param qualifiers  列限定符，支持 family:qualifier 或 qualifier（默认列族）；Nil 则拉整行
   * @param scan        HBase scan对象
   * @return Map结果集
   */
  def scanMapList(tableName: String, qualifiers: Seq[String], scan: Scan): ListBuffer[Map[String, String]] = {
    requireNonEmpty(tableName, scan)(s"参数不合法，scanMapList ${hbaseClusterUrl(keyNum)}.${tableName}失败.")
    this.applyMapQualifiers(scan, qualifiers: _*)

    val list = ListBuffer[Map[String, String]]()
    var rsScanner: ResultScanner = null
    tryFinallyWithReturn {
      rsScanner = this.scanResultScanner(tableName, scan)
      if (rsScanner != null) {
        rsScanner.foreach(rs => {
          val map = this.hbaseRow2Map(rs)
          if (map.isDefined) list += map.get
        })
      }
      logInfo(s"HBase scanMapList ${hbaseClusterUrl(keyNum)}.${tableName}执行成功, 总计${list.size}条")
      list
    } {
      this.closeResultScanner(rsScanner)
    }(this.logger, "HBase scanMapList",
      s"scanMapList ${hbaseClusterUrl(keyNum)}.${tableName}执行失败",
      "关闭HBase table对象或ResultScanner失败")
  }

  /**
   * 多线程并发 ScanMapList（rowKey 区间），按数值型 rowKey 切分子区间后并行 scan
   */
  def scanMapListAsync(tableName: String, threadNum: Int, startRow: String, endRow: String): ListBuffer[Map[String, String]] = {
    require(threadNum > 0, s"线程数量必须大于0，当前值：$threadNum")
    if (threadNum <= 1) return this.scanMapList(tableName, startRow, endRow)

    val ranges = HBaseUtils.splitRowKeyRange(startRow, endRow, threadNum)
    if (ranges.size <= 1) return this.scanMapList(tableName, startRow, endRow)

    val result = ListBuffer[Map[String, String]]()
    ThreadUtils.parallelProcess(ranges, ranges.size) { partition =>
      partition.flatMap { case (s, e) => this.scanMapList(tableName, s, e) }
    }.foreach(result ++= _)
    result
  }

  /**
   * 多线程并发 ScanMapList，当 Scan 含 start/stop row 时按区间切分；否则退化为单线程 scanMapList
   */
  def scanMapListAsync(tableName: String, threadNum: Int, scan: Scan): ListBuffer[Map[String, String]] = {
    require(threadNum > 0, s"线程数量必须大于0，当前值：$threadNum")
    if (threadNum <= 1) return this.scanMapList(tableName, scan)

    val startRow = if (scan.getStartRow != null) Bytes.toString(scan.getStartRow) else ""
    val stopRow = if (scan.getStopRow != null) Bytes.toString(scan.getStopRow) else ""
    if (StringUtils.isBlank(startRow) || StringUtils.isBlank(stopRow)) {
      return this.scanMapList(tableName, scan)
    }

    val ranges = HBaseUtils.splitRowKeyRange(startRow, stopRow, threadNum)
    if (ranges.size <= 1) return this.scanMapList(tableName, scan)

    val result = ListBuffer[Map[String, String]]()
    ThreadUtils.parallelProcess(ranges, ranges.size) { partition =>
      partition.flatMap { case (s, e) =>
        val subScan = new Scan(scan)
        subScan.setStartRow(Bytes.toBytes(s))
        subScan.setStopRow(Bytes.toBytes(e))
        this.scanMapList(tableName, subScan)
      }
    }.foreach(result ++= _)
    result
  }

  /**
   * 多线程并发 Scan（rowKey 区间），按数值型 rowKey 切分子区间后并行 scan
   */
  def scanAsync[T <: HBaseBaseBean[T] : ClassTag](tableName: String, threadNum: Int, startRow: String, endRow: String): ListBuffer[T] = {
    require(threadNum > 0, s"线程数量必须大于0，当前值：$threadNum")
    if (threadNum <= 1) return this.scan[T](tableName, startRow, endRow)

    val ranges = HBaseUtils.splitRowKeyRange(startRow, endRow, threadNum)
    if (ranges.size <= 1) return this.scan[T](tableName, startRow, endRow)

    val result = ListBuffer[T]()
    ThreadUtils.parallelProcess(ranges, ranges.size) { partition =>
      partition.flatMap { case (s, e) => this.scan[T](tableName, s, e) }
    }.foreach(result ++= _)
    result
  }

  /**
   * 多线程并发 Scan，当 Scan 含 start/stop row 时按区间切分；否则退化为单线程 scan
   */
  def scanAsync[T <: HBaseBaseBean[T] : ClassTag](tableName: String, threadNum: Int, scan: Scan): ListBuffer[T] = {
    require(threadNum > 0, s"线程数量必须大于0，当前值：$threadNum")
    if (threadNum <= 1) return this.scan[T](tableName, scan)

    val startRow = if (scan.getStartRow != null) Bytes.toString(scan.getStartRow) else ""
    val stopRow = if (scan.getStopRow != null) Bytes.toString(scan.getStopRow) else ""
    if (StringUtils.isBlank(startRow) || StringUtils.isBlank(stopRow)) {
      return this.scan[T](tableName, scan)
    }

    val ranges = HBaseUtils.splitRowKeyRange(startRow, stopRow, threadNum)
    if (ranges.size <= 1) return this.scan[T](tableName, scan)

    val result = ListBuffer[T]()
    ThreadUtils.parallelProcess(ranges, ranges.size) { partition =>
      partition.flatMap { case (s, e) =>
        val subScan = new Scan(scan)
        subScan.setStartRow(Bytes.toBytes(s))
        subScan.setStopRow(Bytes.toBytes(e))
        this.scan[T](tableName, subScan)
      }
    }.foreach(result ++= _)
    result
  }

  /**
   * 获取Configuration实例
   *
   * @return HBase Configuration对象
   */
  def getConfiguration: Configuration = this.configuration

  /**
   * 用于初始化全局唯一的HBase connection
   */
  @Internal
  def initConnection: Connection = {
    tryWithReturn {
      val user = hbaseUser(keyNum)
      var secureHadoopUser: User.SecureHadoopUser = null

      if (noEmpty(user)) {
        logInfo(s"hbase访问用户名：$user")
        val ugi = UserGroupInformation.createRemoteUser(user)
        secureHadoopUser = new User.SecureHadoopUser(ugi);
      }

      ConnectionFactory.createConnection(this.getConfiguration, secureHadoopUser)
    }(logger, s"成功创建HBase ${hbaseClusterUrl(keyNum)}集群connection.", s"获取HBase ${hbaseClusterUrl(keyNum)}集群connection失败.")
  }

  /**
   * 根据keyNum获取指定HBase集群的connection
   */
  def getConnection: Connection = this.connection

  /**
   * 内部使用：按 JavaBean 字段构建 Get，仅投影有效列（跳过 @FieldName(disuse=true)）
   */
  @Internal
  private[this] def buildGet[T <: HBaseBaseBean[T] : ClassTag](rowKey: String): Get = {
    val get = HBaseConnector.buildGet(rowKey)
    this.applyBeanColumns[T](get)
    get
  }

  /**
   * 内部使用：按 JavaBean 字段构建 Scan，仅投影有效列（跳过 @FieldName(disuse=true)）
   */
  @Internal
  private[this] def buildScan[T <: HBaseBaseBean[T] : ClassTag](startRow: String, endRow: String): Scan = {
    val scan = HBaseConnector.buildScan(startRow, endRow)
    this.applyBeanColumns[T](scan)
    scan
  }

  /**
   * 若 Get 尚未指定列族/列，则按 Map qualifiers 投影部分列
   */
  @Internal
  private[this] def applyMapQualifiers(get: Get, qualifiers: String*): Unit = {
    if (get == null || qualifiers == null || qualifiers.isEmpty) return
    val familyMap = get.getFamilyMap
    if (familyMap != null && !familyMap.isEmpty) return
    qualifiers.foreach { column =>
      val (family, qualifier) = this.parseFamilyQualifier(column)
      if (StringUtils.isNotBlank(family) && StringUtils.isNotBlank(qualifier)) {
        get.addColumn(family.getBytes(StandardCharsets.UTF_8), qualifier.getBytes(StandardCharsets.UTF_8))
      }
    }
  }

  /**
   * 若 Scan 尚未指定列族/列，则按 Map qualifiers 投影部分列
   */
  @Internal
  private[this] def applyMapQualifiers(scan: Scan, qualifiers: String*): Unit = {
    if (scan == null || qualifiers == null || qualifiers.isEmpty) return
    val familyMap = scan.getFamilyMap
    if (familyMap != null && !familyMap.isEmpty) return
    qualifiers.foreach { column =>
      val (family, qualifier) = this.parseFamilyQualifier(column)
      if (StringUtils.isNotBlank(family) && StringUtils.isNotBlank(qualifier)) {
        scan.addColumn(family.getBytes(StandardCharsets.UTF_8), qualifier.getBytes(StandardCharsets.UTF_8))
      }
    }
  }

  @Internal
  private[this] def parseFamilyQualifier(column: String): (String, String) = {
    if (StringUtils.isBlank(column)) return (null, null)
    val idx = column.indexOf(':')
    if (idx > 0 && idx < column.length - 1) {
      (column.substring(0, idx), column.substring(idx + 1))
    } else {
      (familyName(keyNum), column)
    }
  }

  /**
   * 若 Get 尚未指定列族/列，则按 Bean 字段投影部分列
   */
  @Internal
  private[fire] def applyBeanColumns[T <: HBaseBaseBean[T] : ClassTag](get: Get): Unit = {
    if (get == null || !this.getProjection[T]) return
    val familyMap = get.getFamilyMap
    if (familyMap != null && !familyMap.isEmpty) return
    this.queryableColumns[T]().foreach { case (family, qualifier) =>
      get.addColumn(family, qualifier)
    }
  }

  /**
   * 若 Scan 尚未指定列族/列，则按 Bean 字段投影部分列
   */
  @Internal
  private[fire] def applyBeanColumns[T <: HBaseBaseBean[T] : ClassTag](scan: Scan): Unit = {
    if (scan == null || !this.getProjection[T]) return
    val familyMap = scan.getFamilyMap
    if (familyMap != null && !familyMap.isEmpty) return
    this.queryableColumns[T]().foreach { case (family, qualifier) =>
      scan.addColumn(family, qualifier)
    }
  }

  /**
   * 解析并缓存 Bean 对应的可查询列（复用 getFieldNameMap，按 Class 缓存）
   */
  @Internal
  private[this] def queryableColumns[T <: HBaseBaseBean[T] : ClassTag](): Array[(Array[Byte], Array[Byte])] = {
    val clazz = getGeneric[T]("HBaseConnector.queryableColumns")
    var columns = this.cacheQueryableColumns.get(clazz)
    if (columns == null) {
      val fieldMap = this.getFieldNameMap[T]()
      val multiVersion = this.getMultiVersion[T]
      val builder = ListBuffer[(Array[Byte], Array[Byte])]()
      val it = fieldMap.entrySet().iterator()

      while (it.hasNext) {
        val entry = it.next()
        val field = entry.getValue
        val decl = field.getDeclaringClass
        // getFieldNameMap 会把 MultiVersionsBean 的字段一并并入，投影时需过滤：
        // 1) decl.isAssignableFrom(clazz)：字段声明在 T 或其父类上（如 Student / HBaseBaseBean），应投影
        // 2) 仅当开启 multiVersion 时，才投影 MultiVersionsBean 上的列（如 multiFields），避免单版本查询多拉无关列
        val include = decl.isAssignableFrom(clazz) || (multiVersion && (decl == classOf[MultiVersionsBean] || classOf[MultiVersionsBean].isAssignableFrom(decl)))
        if (include) {
          // fieldMap 的 key 格式为 "family:qualifier"，拆成列族与列名后转为字节，供 Get/Scan.addColumn 使用
          val fq = entry.getKey
          val idx = fq.indexOf(':')
          if (idx > 0 && idx < fq.length - 1) {
            val family = fq.substring(0, idx)
            val qualifier = fq.substring(idx + 1)
            builder += ((family.getBytes(StandardCharsets.UTF_8), qualifier.getBytes(StandardCharsets.UTF_8)))
          }
        }
      }

      columns = builder.toArray
      val existing = this.cacheQueryableColumns.putIfAbsent(clazz, columns)
      if (existing != null) columns = existing
    }
    columns
  }

  /**
   * 将class中的field转为map映射
   *
   * @return 名称与字段的映射map
   */
  @Internal
  private[this] def getFieldNameMap[T <: HBaseBaseBean[T] : ClassTag](): JMap[String, Field] = {
    val clazz = getGeneric[T]("HBaseConnector.getFieldNameMap")
    if (!this.cacheFieldMap.containsKey(clazz)) {
      val beanFields = ReflectionUtils.getAllFields(clazz).filter(t => !t._2.toString.contains(" final "))
      val multiBeanFields = ReflectionUtils.getAllFields(classOf[MultiVersionsBean]).filter(t => !t._2.toString.contains(" final "))
      val allFields = multiBeanFields ++ beanFields
      if (allFields != null) {
        val fieldMap = Maps.newHashMapWithExpectedSize[String, Field](allFields.size)

        if (allFields != null) {
          allFields.values.filter(_ != null).foreach(field => {
            val fieldName = field.getAnnotation(classOf[FieldName])
            // 与 Put 一致：显式 disuse=true 的字段不参与映射与列投影
            if (fieldName == null || !fieldName.disuse) {
              var family = if (fieldName != null) fieldName.family else ""
              var qualifier = if (fieldName != null) fieldName.value else ""
              if (StringUtils.isBlank(family)) family = familyName(keyNum)
              if (StringUtils.isBlank(qualifier)) qualifier = field.getName
              fieldMap.put(family + ":" + qualifier, field)
            }
          })
        }
        cacheFieldMap.put(clazz, fieldMap)
      }
    }

    this.cacheFieldMap.get(clazz)
  }

  /**
   * 为指定对象的field赋值
   *
   * @param obj   目标对象
   * @param field 指定filed
   * @param value byte类型的数据
   */
  @Internal
  private def setFieldBytesValue[T <: HBaseBaseBean[T]](obj: T, field: Field, value: Array[Byte]): Unit = {
    tryWithLog {
      if (field != null && value != null && value.nonEmpty) {
        ReflectionUtils.setAccessible(field)
        val toValue = field.getType match {
          case fieldType if fieldType eq classOf[JString] => Bytes.toString(value)
          case fieldType if fieldType eq classOf[JInt] => Bytes.toInt(value)
          case fieldType if fieldType eq classOf[JDouble] => Bytes.toDouble(value)
          case fieldType if fieldType eq classOf[JLong] => Bytes.toLong(value)
          case fieldType if fieldType eq classOf[JBigDecimal] => Bytes.toBigDecimal(value)
          case fieldType if fieldType eq classOf[JFloat] => Bytes.toFloat(value)
          case fieldType if fieldType eq classOf[JBoolean] => Bytes.toBoolean(value)
          case fieldType if fieldType eq classOf[JShort] => Bytes.toShort(value)
          case _ => Bytes.toString(value)
        }
        field.set(obj, toValue)
      } else if (field != null) field.set(obj, null)
    }(this.logger, catchLog = s"为filed ${field}设置赋值过程中出现异常")
  }

  /**
   * 将含有多版本的cell映射为field
   *
   * @param rs       hbase 结果集
   * @param fieldMap 字段映射信息
   */
  @Internal
  private[this] def multiCell2Field[T <: HBaseBaseBean[T] : ClassTag](rs: Result, fieldMap: JMap[String, Field]): ListBuffer[T] = {
    val clazz = getGeneric[T]("HBaseConnector.multiCell2Field")
    val objList = ListBuffer[T]()
    tryWithLog {
      if (rs != null) {
        rs.rawCells.filter(_ != null).foreach(cell => {
          val obj = new MultiVersionsBean
          val rowKey = new String(CellUtil.cloneRow(cell), StandardCharsets.UTF_8)
          val family = new String(CellUtil.cloneFamily(cell), StandardCharsets.UTF_8)
          val qualifier = new String(CellUtil.cloneQualifier(cell), StandardCharsets.UTF_8)
          val value = CellUtil.cloneValue(cell)
          val field = fieldMap.get(family + ":" + qualifier)
          this.setFieldBytesValue(obj, field, value)
          val idField = ReflectionUtils.getFieldByName(clazz, "rowKey")
          requireNonEmpty(idField)(s"${clazz}中必须有名为rowKey的成员变量")
          idField.set(obj, rowKey)
          if (StringUtils.isNotBlank(obj.getMultiFields)) objList.add(JSONUtils.parseObject[T](obj.getMultiFields))
        })
      }
    }(this.logger, catchLog = s"将多版本json数据转为类型${clazz}过程中发生失败.")
    objList
  }

  /**
   * 将cell中的值转为File的值
   *
   * @param fieldMap 成员变量信息
   * @param rs       hbase查询结果集
   * @return clazz对应的结果实例
   */
  @Internal
  private[this] def cell2Field[T <: HBaseBaseBean[T] : ClassTag](fieldMap: JMap[String, Field], rs: Result): Option[T] = {
    val clazz = getGeneric[T]("HBaseConnector.cell2Field")
    val cells = rs.rawCells
    if (cells == null) return None

    val obj = clazz.newInstance
    tryWithLog {
      val rowKey = convertCells2Fields[T](fieldMap, obj, cells)
      val idField = ReflectionUtils.getFieldByName(clazz, "rowKey")
      requireNonEmpty(idField)(s"${clazz}中必须有名为rowKey的成员变量")
      ReflectionUtils.setAccessible(idField)
      idField.set(obj, rowKey)
    }(this.logger, catchLog = "将HBase cell中的值转换并赋值给field过程中报错.")

    Some(obj)
  }

  /**
   * 一次循环取出cell中的值赋值给各个field
   *
   * @param obj   对象实例
   * @param cells hbase结果集中的cells集合
   * @return rowkey
   */
  @Internal
  private[this] def convertCells2Fields[T <: HBaseBaseBean[T]](fieldMap: JMap[String, Field], obj: T, cells: Array[Cell]): String = {
    requireNonEmpty(fieldMap, obj)

    var rowKey = ""
    if (cells != null) {
      cells.filter(_ != null).foreach(cell => {
        rowKey = new String(CellUtil.cloneRow(cell), StandardCharsets.UTF_8)
        val family = new String(CellUtil.cloneFamily(cell), StandardCharsets.UTF_8)
        val qualifier = new String(CellUtil.cloneQualifier(cell), StandardCharsets.UTF_8)
        val value = CellUtil.cloneValue(cell)
        val field = fieldMap.get(family + ":" + qualifier)
        this.setFieldBytesValue(obj, field, value)
      })
    }
    rowKey
  }

  /**
   * 将单版本 Result 映射为 Map
   * Map结构：rowKey + family:qualifier -> value（启发式还原后转为 String）
   *
   * @param rs HBase查询结果集
   * @return Map结果，空行返回 None
   */
  @Internal
  private[fire] def hbaseRow2Map(rs: Result): Option[Map[String, String]] = {
    requireNonNull(rs)("参数不合法，HBase Row转为Map失败.")
    if (rs.isEmpty) return None
    val cells = rs.rawCells()
    if (cells == null || cells.isEmpty) return None

    val builder = Map.newBuilder[String, String]
    var rowKey = ""
    cells.filter(_ != null).foreach(cell => {
      rowKey = new String(CellUtil.cloneRow(cell), StandardCharsets.UTF_8)
      val family = new String(CellUtil.cloneFamily(cell), StandardCharsets.UTF_8)
      val qualifier = new String(CellUtil.cloneQualifier(cell), StandardCharsets.UTF_8)
      val valueBytes = CellUtil.cloneValue(cell)
      builder += ((family + ":" + qualifier) -> HBaseConnector.bytesToMapValue(valueBytes))
    })

    if (StringUtils.isNotBlank(rowKey)) {
      builder += ("rowKey" -> rowKey)
      Some(builder.result())
    } else None
  }

  /**
   * 将多条 Result 映射为 Map 列表
   *
   * @param rsArr HBase查询结果集
   * @return Map结果集
   */
  @Internal
  private[fire] def hbaseRow2Map(rsArr: ListBuffer[Result]): ListBuffer[Map[String, String]] = {
    requireNonNull(rsArr)("参数不合法，HBase Row转为Map失败.")
    val mapList = ListBuffer[Map[String, String]]()
    rsArr.filter(rs => rs != null && !rs.isEmpty).foreach(rs => {
      val map = this.hbaseRow2Map(rs)
      if (map.isDefined) mapList += map.get
    })
    mapList
  }

  /**
   * 将结果映射到自定义bean中
   *
   * @param rs HBase查询结果集
   * @return 目标类型实例
   */
  @Internal
  private[fire] def hbaseRow2Bean[T <: HBaseBaseBean[T] : ClassTag](rs: Result): Option[T] = {
    requireNonNull(rs)("参数不合法，HBase Row转为JavaBean失败.")
    val fieldMap = this.getFieldNameMap[T]
    requireNonEmpty(fieldMap)(s"未声明任何成员变量或成员变量未声明注解@FieldName")
    this.cell2Field[T](fieldMap, rs)
  }

  /**
   * 将结果映射到自定义bean中
   *
   * @param rsArr HBase查询结果集
   * @param clazz 映射的目标Class类型
   * @return 目标类型实例
   */
  @Internal
  private[fire] def hbaseRow2Bean[T <: HBaseBaseBean[T] : ClassTag](rsArr: ListBuffer[Result]): ListBuffer[T] = {
    requireNonNull(rsArr)("参数不合法，HBase Row转为JavaBean失败.")
    val fieldMap = this.getFieldNameMap[T]
    requireNonEmpty(fieldMap)(s"未声明任何成员变量或成员变量未声明注解@FieldName")
    val objList = ListBuffer[T]()
    rsArr.filter(rs => rs != null && !rs.isEmpty).foreach(rs => {
      val obj = this.cell2Field[T](fieldMap, rs)
      if (obj.isDefined) objList += obj.get
    })
    objList
  }

  /**
   * 将结果映射到自定义bean中
   *
   * @param rs HBase查询结果集
   * @return 目标类型实例
   */
  @Internal
  private[fire] def hbaseMultiRow2Bean[T <: HBaseBaseBean[T] : ClassTag](rs: Result): ListBuffer[T] = {
    requireNonNull(rs)("参数不合法，HBase MultiRow转为JavaBean失败.")
    val fieldMap = this.getFieldNameMap[T]
    requireNonEmpty(fieldMap)(s"未声明任何成员变量或成员变量未声明注解@FieldName")
    this.multiCell2Field[T](rs, fieldMap)
  }

  /**
   * 将结果映射到自定义bean中
   *
   * @param rsArr HBase查询结果集
   * @param clazz 映射的目标Class类型
   * @return 目标类型实例
   */
  @Internal
  private[fire] def hbaseMultiRow2Bean[T <: HBaseBaseBean[T] : ClassTag](rsArr: ListBuffer[Result], clazz: Class[T]): ListBuffer[T] = {
    requireNonNull(rsArr, clazz)("参数不合法，HBase Row转为JavaBean失败.")
    val fieldMap = getFieldNameMap[T]
    requireNonEmpty(fieldMap)(s"${clazz}中未声明任何成员变量或成员变量未声明注解@FieldName")
    val objList = ListBuffer[T]()
    rsArr.filter(rs => rs != null && !rs.isEmpty).foreach(rs => objList ++= this.multiCell2Field[T](rs, fieldMap))
    objList
  }

  /**
   * 将结果映射到自定义bean中
   *
   * @param it HBase查询结果集
   * @return 目标类型实例
   */
  @Internal
  private[fire] def hbaseRow2BeanList[T <: HBaseBaseBean[T] : ClassTag](it: Iterator[(ImmutableBytesWritable, Result)]): Iterator[T] = {
    val clazz = getGeneric[T]("HBaseConnector.hbaseRow2BeanList")
    requireNonNull(it, clazz)

    val fieldMap = this.getFieldNameMap[T]
    requireNonEmpty(fieldMap)(s"${clazz}中未声明任何成员变量或成员变量未声明注解@FieldName")
    val beanList = ListBuffer[T]()
    tryWithLog {
      it.foreach(t => {
        val cells = t._2.rawCells()
        if (cells != null) {
          val obj = clazz.newInstance()
          val rowKey = this.convertCells2Fields[T](fieldMap, obj, cells)
          val idField = ReflectionUtils.getFieldByName(clazz, "rowKey")
          requireNonEmpty(idField)(s"${clazz}中必须有名为rowKey的成员变量")
          idField.set(obj, rowKey)
          beanList += obj
        }
      })
    }(this.logger, catchLog = "执行hbaseRow2BeanList过程中出现异常")
    beanList.iterator
  }

  /**
   * 将多版本结果映射到自定义bean中
   *
   * @param it HBase查询结果集
   * @return 目标类型实例
   */
  @Internal
  private[fire] def hbaseMultiVersionRow2BeanList[T <: HBaseBaseBean[T] : ClassTag](it: Iterator[(ImmutableBytesWritable, Result)]): Iterator[T] = {
    requireNonNull(it)

    val beanList = ListBuffer[T]()
    tryWithLog {
      it.foreach(t => {
        beanList ++= this.hbaseMultiRow2Bean[T](t._2)
      })
    }(this.logger, catchLog = "将HBase多版本Row转为JavaBean过程中出现异常.")

    beanList.iterator
  }

  /**
   * 将Javabean转为put对象
   *
   * @param obj         对象
   * @param insertEmpty true:插入null字段，false：不插入空字段
   * @return put对象实例
   */
  @Internal
  private[fire] def convert2Put[T <: HBaseBaseBean[T]](obj: T, insertEmpty: Boolean): Put = {
    requireNonEmpty(obj, insertEmpty)("参数不能为空，无法将对象转为HBase Put对象")
    tryWithReturn {
      var tmpObj = obj
      val clazz = tmpObj.getClass
      val rowKeyField = ReflectionUtils.getFieldByName(clazz, "rowKey")
      var rowKeyObj = rowKeyField.get(tmpObj)
      if (rowKeyObj == null) {
        val method = ReflectionUtils.getMethodByName(clazz, "buildRowKey")
        tmpObj = method.invoke(tmpObj).asInstanceOf[T]
        rowKeyObj = rowKeyField.get(tmpObj)
        requireNonEmpty(rowKeyObj)(s"rowKey不能为空，请检查${clazz}中是否实现buildRowKey()方法！")
      }

      val allFields = ReflectionUtils.getAllFields(clazz)
      requireNonEmpty(allFields)(s"在${clazz}中未找到任何成员变量，请检查！")
      val rowKey = rowKeyObj.toString.getBytes(StandardCharsets.UTF_8)
      val put = new Put(rowKey)
      put.setDurability(this.durability)
      allFields.values().foreach(field => {
        val objValue = field.get(obj)
        // 将objValue插入的两种情况：1. 允许插入为空的值；2. 不允许插入为空的值，并且objValue不为空
        if (insertEmpty || (!insertEmpty && objValue != null)) {
          val fieldName = field.getAnnotation(classOf[FieldName])
          var name = ""
          var familyName = ""
          if (fieldName != null && !fieldName.disuse) {
            familyName = fieldName.family
            name = fieldName.value
          }

          // 如果未声明@FieldName注解或者声明了@FieldName注解但同时在注解中的disuse指定为false，则进行字段的转换
          // 如果不满足以上两个条件，则任务当前字段不需要转为Put对象中的qualifier
          if (fieldName == null || (fieldName != null && !fieldName.disuse())) {
            if (StringUtils.isBlank(familyName)) familyName = FireHBaseConf.familyName(keyNum)
            if (StringUtils.isBlank(name)) name = field.getName
            val familyByte = familyName.getBytes(StandardCharsets.UTF_8)
            val qualifierByte = name.getBytes(StandardCharsets.UTF_8)
            if (objValue != null) {
              val objValueStr = objValue.toString
              val toBytes = field.getType match {
                case fieldType if fieldType eq classOf[JString] => Bytes.toBytes(objValueStr)
                case fieldType if fieldType eq classOf[JInt] => Bytes.toBytes(JInt.parseInt(objValueStr))
                case fieldType if fieldType eq classOf[JDouble] => Bytes.toBytes(JDouble.parseDouble(objValueStr))
                case fieldType if fieldType eq classOf[JLong] => Bytes.toBytes(JLong.parseLong(objValueStr))
                case fieldType if fieldType eq classOf[JBigDecimal] => Bytes.toBytes(new JBigDecimal(objValueStr))
                case fieldType if fieldType eq classOf[JFloat] => Bytes.toBytes(JFloat.parseFloat(objValueStr))
                case fieldType if fieldType eq classOf[JBoolean] => Bytes.toBytes(JBoolean.parseBoolean(objValueStr))
                case fieldType if fieldType eq classOf[JShort] => Bytes.toBytes(JShort.parseShort(objValueStr))
                case _ => Bytes.toBytes(objValueStr)
              }
              put.addColumn(familyByte, qualifierByte, toBytes)
            } else {
              put.addColumn(familyByte, qualifierByte, null)
            }
          }
        }
      })
      put
    }(this.logger, catchLog = "将JavaBean转为HBase Put对象过程中出现异常.")
  }

  /**
   * 提供给fire-spark引擎的工具方法
   *
   * @param obj 继承自HBaseBaseBean的子类实例
   * @return HBaseBaseBean的子类实例
   */
  @Internal
  private[fire] def convert2PutTuple[T <: HBaseBaseBean[T]](obj: T, insertEmpty: Boolean = true): (ImmutableBytesWritable, Put) = {
    (new ImmutableBytesWritable(), convert2Put[T](obj, insertEmpty))
  }

  /**
   * 获取类注解HConfig中的nullable
   */
  @Internal
  private[fire] def getNullable[T <: HBaseBaseBean[T] : ClassTag]: Boolean = {
    val hConfig = this.getHConfig[T]
    if (hConfig == null) return true
    hConfig.nullable()
  }

  /**
   * 获取类注解HConfig中的multiVersion
   */
  @Internal
  private[fire] def getMultiVersion[T <: HBaseBaseBean[T] : ClassTag]: Boolean = {
    val hConfig = this.getHConfig[T]
    if (hConfig == null) return false
    hConfig.multiVersion()
  }

  /**
   * 获取类注解HConfig中的bulkLoadStagingDir
   */
  @Internal
  private[fire] def getBulkLoadStagingDir[T <: HBaseBaseBean[T] : ClassTag]: String = {
    val hConfig = this.getHConfig[T]
    if (hConfig == null) return ""
    hConfig.bulkLoadStagingDir()
  }

  /**
   * 是否在 BulkLoad 前后删除 staging 目录
   * 优先级：fire.hbase.bulkload.deleteStagingDir（显式配置）> @HConfig.bulkLoadDeleteStagingDir > 默认 false
   */
  @Internal
  private[fire] def resolveBulkLoadDeleteStagingDir[T <: HBaseBaseBean[T] : ClassTag]: Boolean = {
    val confRaw = PropUtils.getString(FireHBaseConf.HBASE_BULKLOAD_DELETE_STAGING_DIR, "", this.keyNum)
    if (StringUtils.isNotBlank(confRaw)) {
      return confRaw.toBoolean
    }

    val hConfig = this.getHConfig[T]
    if (hConfig != null) hConfig.bulkLoadDeleteStagingDir() else false
  }

  /**
   * 解析 BulkLoad staging 完整路径
   */
  @Internal
  private[fire] def resolveBulkLoadStagingDir[T <: HBaseBaseBean[T] : ClassTag](tableName: String, stagingDirParam: String = ""): String = {
    val fromConf = FireHBaseConf.bulkLoadStagingDir(this.keyNum)
    val fromAnno = this.getBulkLoadStagingDir[T]
    val base = Seq(fromConf, fromAnno, stagingDirParam).find(StringUtils.isNotBlank).getOrElse {
      throw new IllegalArgumentException(
        s"未配置 BulkLoad staging 目录，请设置 fire.hbase.bulkload.stagingDir（或 @HConfig.bulkLoadStagingDir / 方法入参 stagingDir），keyNum=${this.keyNum}")
    }

    // 将表名中的namespace统一替换，hdfs路径不允许出现冒号等非法字符
    val safeTableName = tableName.replace(':', '_')
    s"${base.replaceAll("/+$", "")}/fire_bulkload_${safeTableName}"
  }

  /**
   * 获取类注解 HConfig 中的 projection（是否开启 Bean 字段投影查询）
   */
  @Internal
  private[fire] def getProjection[T <: HBaseBaseBean[T] : ClassTag]: Boolean = {
    val hConfig = this.getHConfig[T]
    if (hConfig == null) return false
    hConfig.projection()
  }

  /**
   * 获取类上声明的HConfig注解，并加入缓存以降低反射带来的性能开销
   */
  @Internal
  private[fire] def getHConfig[T <: HBaseBaseBean[T] : ClassTag]: HConfig = {
    val clazz = getGeneric[T]("HBaseConnector.getHConfig")
    if (!this.cacheHConfigMap.containsKey(clazz)) {
      val hConfig = clazz.getAnnotation(classOf[HConfig])
      if (hConfig != null) {
        this.cacheHConfigMap.put(clazz, hConfig)
      }
    }
    this.cacheHConfigMap.get(clazz)
  }

  /**
   * 根据keyNum获取对应配置的durability
   */
  @Internal
  private[this] def initDurability: Durability = {
    val hbaseDurability = FireHBaseConf.hbaseDurability(keyNum)

    // 将匹配到的配置转为Durability对象
    hbaseDurability.toUpperCase match {
      case "ASYNC_WAL" => Durability.ASYNC_WAL
      case "FSYNC_WAL" => Durability.FSYNC_WAL
      case "SKIP_WAL" => Durability.SKIP_WAL
      case "SYNC_WAL" => Durability.SYNC_WAL
      case _ => Durability.USE_DEFAULT
    }
  }

  /**
   * 创建HBase表
   *
   * @param tableName
   * 表名
   * @param families
   * 列族
   */
  private[fire] def createTable(tableName: String, families: String*): Unit = {
    requireNonEmpty(tableName, families)("执行createTable失败")
    var admin: Admin = null
    tryFinallyWithReturn {
      admin = this.getConnection.getAdmin
      val tbName = TableName.valueOf(tableName)
      if (!admin.tableExists(tbName)) {
        val tableDesc = new HTableDescriptor(tbName)
        // 在描述里添加列族
        for (columnFamily <- families) {
          val desc = new HColumnDescriptor(columnFamily)
          // 启用压缩
          desc.setCompressionType(Compression.Algorithm.SNAPPY)
          tableDesc.addFamily(desc)
        }
        admin.createTable(tableDesc)
        addHBaseDatasource(tableName, FOperation.CREATE_TABLE, keyNum)
        // 如果开启表缓存，则更新缓存信息
        if (this.tableExistsCacheEnable && this.tableExists(tableName)) this.cacheTableExistsMap.update(tableName, true)
      }
    } {
      this.closeAdmin(admin)
    }(logger, s"HBase createTable ${hbaseClusterUrl(keyNum)}.${tableName}执行成功",
      s"创建HBase表${hbaseClusterUrl(keyNum)}.${tableName}失败.", closeAdminError)
  }

  /**
   * 删除指定的HBase表
   *
   * @param tableName 表名
   */
  def dropTable(tableName: String): Unit = {
    requireNonEmpty(tableName)("执行dropTable失败")
    var admin: Admin = null
    tryFinallyWithReturn {
      admin = this.getConnection.getAdmin
      val tbName = TableName.valueOf(tableName)
      if (admin.tableExists(tbName)) {
        admin.disableTable(tbName)
        admin.deleteTable(tbName)
        // 如果开启表缓存，则更新缓存信息
        if (this.tableExistsCacheEnable && !this.tableExists(tableName)) this.cacheTableExistsMap.update(tableName, false)
        addHBaseDatasource(tableName, FOperation.DROP_TABLE, keyNum)
      }
    } {
      this.closeAdmin(admin)
    }(this.logger, s"HBase createTable ${hbaseClusterUrl(keyNum)}.${tableName}执行成功",
      s"drop ${hbaseClusterUrl(keyNum)}.${tableName}表操作失败", closeAdminError)
  }

  /**
   * 启用指定的HBase表
   *
   * @param tableName 表名
   */
  def enableTable(tableName: String): Unit = {
    requireNonEmpty(tableName)("执行enableTable失败")
    var admin: Admin = null
    tryFinallyWithReturn {
      admin = this.getConnection.getAdmin
      val tbName = TableName.valueOf(tableName)
      if (admin.tableExists(tbName) && !admin.isTableEnabled(tbName)) {
        admin.enableTable(tbName)
        addHBaseDatasource(tableName, FOperation.ENABLE_TABLE, keyNum)
      }
    } {
      this.closeAdmin(admin)
    }(this.logger, s"HBase enableTable ${hbaseClusterUrl(keyNum)}.${tableName}执行成功",
      s"enable ${hbaseClusterUrl(keyNum)}.${tableName}表失败", closeAdminError)
  }

  /**
   * disable指定的HBase表
   *
   * @param tableName 表名
   */
  def disableTable(tableName: String): Unit = {
    requireNonEmpty(tableName)("执行disableTable失败")
    var admin: Admin = null
    tryFinallyWithReturn {
      admin = this.getConnection.getAdmin
      val tbName = TableName.valueOf(tableName)
      if (admin.tableExists(tbName) && admin.isTableEnabled(tbName)) {
        admin.disableTable(tbName)
        addHBaseDatasource(tableName, FOperation.DISABLE_TABLE, keyNum)
      }
    } {
      this.closeAdmin(admin)
    }(this.logger, s"HBase disableTable ${hbaseClusterUrl(keyNum)}.${tableName}执行成功",
      s"disable ${hbaseClusterUrl(keyNum)}.${tableName}表失败", closeAdminError)
  }

  /**
   * 清空指定的HBase表
   *
   * @param tableName      HBase表名
   * @param preserveSplits 是否保留所有的split信息
   */
  def truncateTable(tableName: String, preserveSplits: Boolean = true): Unit = {
    requireNonEmpty(tableName, preserveSplits)("执行truncateTable失败")
    var admin: Admin = null
    tryFinallyWithReturn {
      admin = this.getConnection.getAdmin
      val tbName = TableName.valueOf(tableName)
      if (admin.tableExists(tbName)) {
        this.disableTable(tableName)
        admin.truncateTable(tbName, preserveSplits)
        addHBaseDatasource(tableName, FOperation.TRUNCATE, keyNum)
      }
    } {
      this.closeAdmin(admin)
    }(this.logger, s"HBase truncateTable ${hbaseClusterUrl(keyNum)}.${tableName}执行成功",
      s"truncate ${hbaseClusterUrl(keyNum)}.${tableName}表失败", closeAdminError)
  }

  /**
   * 释放对象
   *
   * @param admin admin对象实例
   */
  @Internal
  private[this] def closeAdmin(admin: Admin): Unit = {
    tryWithLog {
      if (admin != null) admin.close()
    }(logger, catchLog = "关闭HBase admin对象失败")
  }

  /**
   * 关闭ResultScanner对象
   */
  @Internal
  private[this] def closeResultScanner(rs: ResultScanner): Unit = {
    tryWithLog {
      if (rs != null) rs.close()
    }(this.logger, catchLog = "关闭ResultScanner对象失败", isThrow = false)
  }

  /**
   * 关闭table对象
   */
  def closeTable(table: Table): Unit = {
    tryWithLog {
      if (table != null) table.close()
    }(logger, catchLog = "关闭HBase table对象失败", isThrow = true)
  }

  /**
   * 根据表名获取Table实例
   *
   * @param tableName 表名
   */
  def getTable(tableName: String): Table = {
    tryWithReturn {
      require(this.isExists(tableName), s"表${tableName}不存在，请检查")
      this.getConnection.getTable(TableName.valueOf(tableName))
    }(logger, catchLog = s"HBase getTable操作失败. ${hbaseClusterUrl(keyNum)}.${tableName}")
  }

  /**
   * 判断给定的表名是否存在
   *
   * @param tableName
   * HBase表名
   */
  def isExists(tableName: String): Boolean = {
    if (StringUtils.isBlank(tableName)) return false
    if (this.tableExistsCacheEnable) {
      // 如果走缓存
      if (!this.cacheTableExistsMap.containsKey(tableName)) {
        logDebug(s"已缓存${tableName}是否存在信息，后续将走缓存.")
        this.cacheTableExistsMap.put(tableName, this.tableExists(tableName))
      }
      this.cacheTableExistsMap.get(tableName)
    } else {
      // 不走缓存则每次连接HBase获取表是否存在的信息
      this.tableExists(tableName)
    }
  }

  /**
   * 用于判断HBase表是否存在
   * 注：内部api，每次需连接HBase获取表信息
   */
  @Internal
  private[fire] def tableExists(tableName: String): Boolean = {
    if (StringUtils.isBlank(tableName)) return false
    var admin: Admin = null
    tryFinallyWithReturn {
      admin = this.getConnection.getAdmin
      val isExists = admin.tableExists(TableName.valueOf(tableName))
      logDebug(s"HBase tableExists ${hbaseClusterUrl(keyNum)}.${tableName}获取成功")
      isExists
    } {
      closeAdmin(admin)
    }(logger, catchLog = s"判断HBase表${hbaseClusterUrl(keyNum)}.${tableName}是否存在失败")
  }

  /**
   * 根据多个rowKey删除对应的整行记录
   *
   * @param tableName 表名
   * @param rowKeys   待删除的rowKey集合
   */
  def deleteRows(tableName: String, rowKeys: String*): Unit = {
    if (noEmpty(tableName, rowKeys)) {
      var table: Table = null
      tryFinallyWithReturn {
        table = this.getTable(tableName)

        val deletes = ListBuffer[Delete]()
        rowKeys.filter(StringUtils.isNotBlank).foreach(rowKey => {
          deletes += new Delete(rowKey.getBytes(StandardCharsets.UTF_8))
        })

        table.delete(deletes)
        addHBaseDatasource(tableName, FOperation.DELETE, keyNum)
      } {
        this.closeTable(table)
      }(this.logger, s"HBase deleteRows ${hbaseClusterUrl(keyNum)}.${tableName}执行成功",
        s"执行${tableName}表rowKey删除失败", "close HBase table对象失败")
    }
  }

  /**
   * 批量删除指定RowKey的多个列族
   *
   * @param tableName 表名
   * @param rowKey    rowKey
   * @param families  多个列族
   */
  def deleteFamilies(tableName: String, rowKey: String, families: String*): Unit = {
    if (noEmpty(tableName, rowKey, families)) {
      val delete = new Delete(rowKey.getBytes(StandardCharsets.UTF_8))
      families.filter(StringUtils.isNotBlank).foreach(family => delete.addFamily(family.getBytes(StandardCharsets.UTF_8)))

      var table: Table = null
      tryFinallyWithReturn {
        table = this.getTable(tableName)
        table.delete(delete)
        addHBaseDatasource(tableName, FOperation.DELETE_FAMILY, keyNum)
      } {
        this.closeTable(table)
      }(this.logger, s"HBase deleteFamilies ${hbaseClusterUrl(keyNum)}.${tableName}执行成功",
        s"delete ${hbaseClusterUrl(keyNum)}.${tableName} families failed. RowKey is ${rowKey}, families is ${families}",
        "close HBase table对象出现异常.")
    }
  }

  /**
   * 批量删除指定列族下的多个字段
   *
   * @param tableName  表名
   * @param rowKey     rowKey字段
   * @param family     列族
   * @param qualifiers 列名
   */
  def deleteQualifiers(tableName: String, rowKey: String, family: String, qualifiers: String*): Unit = {
    if (noEmpty(tableName, rowKey, family, qualifiers)) {
      val delete = new Delete(rowKey.getBytes(StandardCharsets.UTF_8))
      qualifiers.foreach(qualifier => delete.addColumns(family.getBytes(StandardCharsets.UTF_8), qualifier.getBytes(StandardCharsets.UTF_8)))
      var table: Table = null

      tryFinallyWithReturn {
        table = this.getTable(tableName)
        table.delete(delete)
        addHBaseDatasource(tableName, FOperation.DELETE_QUALIFIER, keyNum)
      } {
        this.closeTable(table)
      }(this.logger, s"HBase deleteQualifiers ${hbaseClusterUrl(keyNum)}.${tableName}执行成功",
        s"delete ${hbaseClusterUrl(keyNum)}.${tableName} qualifiers failed. RowKey is ${rowKey}, qualifiers is ${qualifiers}", "close HBase table对象出现异常.")
    }
  }

  /**
   * 用于定时reload表是否存在的数据
   */
  @Internal
  private[this] def registerReload(): Unit = {
    if (tableExistsCacheReload(this.keyNum)) {
      ThreadUtils.scheduleWithFixedDelay({
        val start = currentTime
        cacheTableExistsMap.foreach(kv => {
          cacheTableExistsMap.update(kv._1, tableExists(kv._1))
          // 将用到的表信息加入到数据源管理器中
          logDebug(s"定时reload HBase表：${kv._1} 信息成功.")
        })
        logDebug(s"定时reload HBase耗时：${elapsed(start)}")
      }, tableExistCacheInitialDelay(this.keyNum), tableExistCachePeriod(this.keyNum), TimeUnit.SECONDS)
    }
  }

  /**
   * 用于初始化单例的configuration
   */
  @Internal
  override protected[fire] def open(): Unit = {
    val finalConf = if (this.conf != null) this.conf else HBaseConfiguration.create()

    val url = hbaseClusterUrl(keyNum)
    if (StringUtils.isNotBlank(url)) finalConf.set("hbase.zookeeper.quorum", url)

    // 加载hdfs HA相关配置信息，支持跨集群bulk load
    val clusterAlias = FireHBaseConf.hbaseCluster(keyNum)
    FireHDFSConf.hdfsHAConfByCluster(clusterAlias).foreach(kv => {
      logInfo(s"hbase load hdfs.ha.conf.${clusterAlias}: key=${kv._1} value=${kv._2}")
      finalConf.set(kv._1, kv._2)
    })

    // 以 spark.fire.hbase.conf.xxx[keyNum] 开头的配置（可覆盖上方 HA 项）
    PropUtils.sliceKeysByNum(hbaseConfPrefix, keyNum).foreach(kv => {
      logInfo(s"hbase configuration: key=${kv._1} value=${kv._2}")
      finalConf.set(kv._1, kv._2)
    })

    requireNonEmpty(finalConf.get("hbase.zookeeper.quorum"))(s"未配置HBase集群信息，请通过以下参数指定：spark.hbase.cluster[$keyNum]=xxx")
    this.configuration = finalConf
  }

  /**
   * connector关闭
   */
  override protected def close(): Unit = {
    if (this.connection != null && !this.connection.isClosed) {
      this.connection.close()
      logDebug(s"释放HBase connection成功. keyNum=$keyNum")
    }
  }

  /**
   * 获取HBaseBaseBean子类@HConfig中的versions的值
   */
  @Internal
  private[this] def getVersions[T <: HBaseBaseBean[T] : ClassTag]: Int = {
    val clazz = getGeneric[T]("HBaseConnector.getVersions")
    val hConfig = ReflectionUtils.getClassAnnotation(clazz, classOf[HConfig])
    // 仅当开启多版本的情况下versions的值才有效
    if (hConfig == null || !this.getMultiVersion[T]) 1 else hConfig.asInstanceOf[HConfig].versions
  }

  /**
   * 为Get对象设置获取最大的版本数
   */
  @Internal
  private[fire] def getMaxVersions[T <: HBaseBaseBean[T] : ClassTag](gets: Get*): Unit = {
    val versions = this.getVersions[T]
    if (this.getMultiVersion[T] && versions > 1) gets.foreach(get => get.setMaxVersions(versions))
  }

  /**
   * 为Scan对象设置获取最大的版本数
   */
  @Internal
  private[fire] def setScanMaxVersions[T <: HBaseBaseBean[T] : ClassTag](scan: Scan): Unit = {
    val versions = this.getVersions[T]
    if (this.getMultiVersion[T] && versions > 1) scan.setMaxVersions(versions)
  }
}

/**
 * 用于单例构建伴生类HBaseConnector的实例对象
 * 每个HBaseConnector实例使用keyNum作为标识，并且与每个HBase集群一一对应
 */
object HBaseConnector extends ConnectorFactory[HBaseConnector] with HBaseFunctions {

  /** 用于区分 getMapList(rowKeys*) 与 getMapList(gets*) 的重载 */
  implicit val canOverload: Boolean = true

  /**
   * 无 schema 时：合法可读 UTF-8 优先按字符串；否则按定长二进制 Boolean/Short/Int/Long，再尝试 BigDecimal
   */
  private[fire] def bytesToMapValue(valueBytes: Array[Byte]): String = {
    if (valueBytes == null || valueBytes.isEmpty) return null

    // HBase Bytes.toBytes(boolean): true=-1(0xFF), false=0 —— 须优先于字符串判断（0xFF 会被误当成字符）
    if (valueBytes.length == 1 && (valueBytes(0) == 0 || valueBytes(0) == -1)) {
      return String.valueOf(Bytes.toBoolean(valueBytes))
    }

    // 短字符串（如 "root"/"java"）长度恰好为 2/4/8，必须先按文本识别，避免被当成 Short/Int/Long
    if (isLikelyUtf8Text(valueBytes)) return Bytes.toString(valueBytes)

    val decoded: Object = valueBytes.length match {
      case 1 => Boolean.box(Bytes.toBoolean(valueBytes))
      case 2 => Short.box(Bytes.toShort(valueBytes))
      case 4 => JInt.valueOf(Bytes.toInt(valueBytes))
      case 8 => JLong.valueOf(Bytes.toLong(valueBytes))
      case _ =>
        // Bytes.toBytes(BigDecimal) = 4字节scale + unscaled；scale 通常较小
        try {
          val scale = Bytes.toInt(valueBytes)
          if (scale >= 0 && scale <= 100 && valueBytes.length > 4) Bytes.toBigDecimal(valueBytes)
          else Bytes.toString(valueBytes)
        } catch {
          case _: Throwable => Bytes.toString(valueBytes)
        }
    }

    if (decoded == null) null else String.valueOf(decoded)
  }

  /**
   * 判断是否为合法可读 UTF-8 文本（排除控制字符；非法 UTF-8 不算文本）
   */
  private[this] def isLikelyUtf8Text(bytes: Array[Byte]): Boolean = {
    if (bytes == null || bytes.isEmpty) return false
    if (bytes.exists { b =>
      val c = b & 0xff
      c < 0x09 || (c > 0x0d && c < 0x20) || c == 0x7f
    }) return false
    try {
      val decoder = StandardCharsets.UTF_8.newDecoder()
      decoder.onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
      decoder.onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT)
      decoder.decode(java.nio.ByteBuffer.wrap(bytes))
      true
    } catch {
      case _: CharacterCodingException => false
    }
  }

  /**
   * 创建HBaseConnector
   */
  override protected def create(conf: Any = null, keyNum: Int = KeyNum._1): HBaseConnector = {
    requireNonEmpty(keyNum)
    val connector = new HBaseConnector(conf.asInstanceOf[Configuration], keyNum)
    logDebug(s"创建HBaseConnector实例成功. keyNum=$keyNum")
    connector
  }

  /**
   * 添加HBase血缘信息
   *
   * @param tableName
   * HBase表名
   * @param operation
   * 操作类型
   * @param keyNum
   * keyNum
   */
  def addHBaseDatasource(tableName: String, operation: FOperation, keyNum: Int): Unit = {
    if (FireFrameworkConf.lineageEnable) {
      HbaseConnectorParser.addDatasource(HBASE, hbaseClusterUrl(keyNum), tableName, "", operation)
    }
  }
}
