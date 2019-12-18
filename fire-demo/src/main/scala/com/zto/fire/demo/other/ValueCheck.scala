package com.zto.fire.demo.other

import java.util

import com.zto.fire.common.util.ValueUtils
import com.zto.fire.core.BaseSparkCore
import com.zto.fire.core.ext.SparkExt._
import com.zto.fire.demo.bean.Student

/**
 * 用于测试非空校验的任务，通过this.value.isEmpty可验证值、集合、map、rdd、Dataset是否为空
 * 也可通过ValueUtils.isEmpty做校验
 *
 * @since 0.4.1
 * @author ChengLong 2019年12月18日 16:38:31
 */
object ValueCheck extends BaseSparkCore {

  def test(int: Int, str: String, list: List[String]): Unit = {
    // 多个参数校验，存在为空的返回true
    println("test() " + this.value.isExistsEmpty(int, str, list))
  }

  override def process: Unit = {
    val rdd = this.spark.parallelize(1 to 0)
    // rdd为空校验，为null或记录数为0
    println("rdd " + this.value.isEmpty(rdd))
    // 或者通过工具类方式校验，该种方式支持在非Spark任务类中使用
    println("rdd " + ValueUtils.isEmpty(rdd))

    val ds = this.spark.createDataFrame(new util.ArrayList[Student](), classOf[Student])
    // Dataset为空校验，为null或记录数为0
    println("ds " + this.value.isEmpty(ds))

    // java/scala集合为null或size=0校验
    println("list " + this.value.isEmpty(new util.ArrayList[String]()))
    // java/scala map为null或size=0校验
    println("map " + this.value.isEmpty(Map[String, Int]()))
    // 字符串为null或空串
    println("string " + this.value.isEmpty(""))
    this.test(1, "", null)

    // 当产生为空时，抛出异常信息
    this.value.requireNonNull(null, "参数不能为空")
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }

}
