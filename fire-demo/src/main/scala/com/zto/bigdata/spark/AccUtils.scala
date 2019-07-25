package com.zto.bigdata.spark

import java.nio.ByteBuffer

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output

import scala.reflect.ClassTag

object AccUtils extends Serializable {

  def serialize[T: ClassTag](value: T): Array[Byte]=
    try {
      val kryo = new Kryo
      val buffer = new Array[Byte](20480)
      val output = new Output(buffer)
      kryo.writeClassAndObject(output, value)
      output.toBytes
    } catch {
      case e: Exception =>
        return null
    }

  /**
    * 16进制的字符串表示转成字节数组
    *
    * @param hexString
    * 16进制格式的字符串
    * @return 转换后的字节数组
    **/
  def toByteArray(hexString: String): Array[Byte] = {
    val tmphexString = hexString.toLowerCase
    val byteArray = new Array[Byte](tmphexString.length / 2)
    var k = 0
    var i = 0
    while ( {
      i < byteArray.length
    }) { //因为是16进制，最多只会占用4位，转换成字节需要两个16进制的字符，高位在先
      val high = (Character.digit(tmphexString.charAt(k), 16) & 0xff).toByte
      val low = (Character.digit(tmphexString.charAt(k + 1), 16) & 0xff).toByte
      byteArray(i) = (high << 4 | low).toByte
      k += 2

      {
        i += 1;
        i - 1
      }
    }
    byteArray
  }

  /**
    * 字节数组转成16进制表示格式的字符串
    *
    * @param byteArray
    * 需要转换的字节数组
    * @return 16进制表示格式的字符串
    **/
  def toHexString(byteArray: Array[Byte]): String = {
    if (byteArray == null || byteArray.length < 1) throw new IllegalArgumentException("this byteArray must not be null or empty")
    val hexString = new StringBuilder
    var i = 0
    while ( {
      i < byteArray.length
    }) {
      if ((byteArray(i) & 0xff) < 0x10) { //0~F前面不零
        hexString.append("0")
      }
      hexString.append(Integer.toHexString(0xFF & byteArray(i)))

      {
        i += 1;
        i - 1
      }
    }
    hexString.toString.toLowerCase
  }
}
