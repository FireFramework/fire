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

package com.zto.fire.hbase.utils

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64

/**
  * HBase 操作工具类
  *
  * @author ChengLong 2019-6-23 13:36:16
  */
private[fire] object HBaseUtils {

  /**
    * 将scan对象转为String
    *
    * @param scan
    * @return
    */
  def convertScanToString(scan: Scan): String = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  /**
   * 将 rowKey 区间切分为多个子区间，用于 scan 并发。
   * 优先按数值型 rowKey 均分；非数值型 rowKey 无法安全切分时返回原区间。
   */
  def splitRowKeyRange(startRow: String, stopRow: String, partitions: Int): Seq[(String, String)] = {
    if (partitions <= 1 || StringUtils.isBlank(startRow) || StringUtils.isBlank(stopRow)) {
      return Seq((startRow, stopRow))
    }
    try {
      val start = BigInt(startRow)
      val stop = BigInt(stopRow)
      if (start >= stop) return Seq((startRow, stopRow))
      val range = stop - start
      if (range < BigInt(partitions)) {
        return (0.toInt.to(range.toInt)).map(i => {
          val key = (start + BigInt(i)).toString
          (key, key)
        })
      }
      (0 until partitions).map { i =>
        val s = start + range * BigInt(i) / BigInt(partitions)
        val e = if (i == partitions - 1) stop else start + range * BigInt(i + 1) / BigInt(partitions) - 1
        (s.toString, e.toString)
      }
    } catch {
      case _: NumberFormatException => Seq((startRow, stopRow))
    }
  }

  /**
    * 将给定的字符串补齐指定的位数
    */
  def appendString(str: String, char: String, length: Int): String = {
    if (StringUtils.isNotBlank(str) && StringUtils.isNotBlank(char) && length > str.length) {
      val sb: StringBuilder = new StringBuilder(str)
      var i: Int = 0
      while (i < length - str.length) {
        sb.append(char)
        i += 1
      }
      sb.toString
    } else if (length == str.length) {
      str
    } else if (length < str.length && length > 0) {
      str.substring(0, length)
    } else {
      ""
    }
  }
}
