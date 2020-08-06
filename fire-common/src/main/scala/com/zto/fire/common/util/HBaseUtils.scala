package com.zto.fire.common.util

import com.zto.fire.common.anno.FieldName
import com.zto.fire.common.conf.FireHBaseConf
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}

import scala.collection.JavaConversions
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

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

  /**
    * 将Bean转为bytes
    *
    * @param obj
    * @param updateNull
    * 为空的也将被覆盖
    * @tparam T
    * @return
    */
  def bean2Bytes[T: ClassTag](obj: T, updateNull: Boolean = true): (Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])]) = {
    if (obj == null || obj.getClass == null) throw new IllegalArgumentException("对象不能为空")
    val clazz = obj.getClass
    clazz.getMethod("buildRowKey").invoke(obj)
    var rowKey = ""
    val arrays = ArrayBuffer[(Array[Byte], Array[Byte], Array[Byte])]()

    JavaConversions.mapAsScalaMap(ReflectionUtils.getAllFields(clazz)).foreach(t => {
      val key = t._1
      val field = t._2
      val objValue = field.get(obj)
      if (StringUtils.isBlank(rowKey) && "rowKey".equals(key)) {
        rowKey = field.get(obj).asInstanceOf[String]
      }
      val fieldName = clazz.getAnnotation(classOf[FieldName])
      val famliyByte = if (fieldName != null && StringUtils.isNotBlank(fieldName.family())) fieldName.family().getBytes else FireHBaseConf.familyName.getBytes
      val goOn = if (fieldName == null) true else fieldName != null && !fieldName.disuse()
      if (goOn) {
        val keyByte = key.getBytes
        if (objValue != null) {
          val objValueStr = objValue.toString
          val fieldType = field.getType
          if (fieldType eq classOf[String]) arrays += ((famliyByte, keyByte, Bytes.toBytes(objValueStr)))
          else if (fieldType eq classOf[Integer]) arrays += ((famliyByte, keyByte, Bytes.toBytes(objValueStr.toInt)))
          else if (fieldType eq classOf[Double]) arrays += ((famliyByte, keyByte, Bytes.toBytes(objValueStr.toDouble)))
          else if (fieldType eq classOf[Long]) arrays += ((famliyByte, keyByte, Bytes.toBytes(objValueStr.toLong)))
          else if (fieldType eq classOf[BigDecimal]) arrays += ((famliyByte, keyByte, Bytes.toBytes(new java.math.BigDecimal(objValueStr))))
          else if (fieldType eq classOf[Float]) arrays += ((famliyByte, keyByte, Bytes.toBytes(objValueStr.toFloat)))
          else if (fieldType eq classOf[Boolean]) arrays += ((famliyByte, keyByte, Bytes.toBytes(objValueStr.toBoolean)))
          else if (fieldType eq classOf[Short]) arrays += ((famliyByte, keyByte, Bytes.toBytes(objValueStr.toShort)))
        } else if (updateNull) {
          arrays += ((famliyByte, keyByte, null))
        }
      }
    })
    (rowKey.getBytes, arrays.toArray)
  }
}
