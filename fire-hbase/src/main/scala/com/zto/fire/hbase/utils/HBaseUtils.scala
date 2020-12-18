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
object HBaseUtils {

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
    * 将给定的字符串补齐指定的位数
    *
    * @param str
    * @param length
    * @return
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
