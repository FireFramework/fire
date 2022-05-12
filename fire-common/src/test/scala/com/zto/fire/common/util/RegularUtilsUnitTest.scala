package com.zto.fire.common.util

import com.zto.fire.common.anno.TestStep
import org.junit.Test

import java.io.StringReader
import java.util.Properties

/**
 * 常用的正则表达式
 *
 * @author ChengLong 2022-05-12 17:20:55
 * @since fire 2.2.2
 */
class RegularUtilsUnitTest {

  @Test
  @TestStep(step = 1, desc = "@Config注解中的注释解析单元测试")
  def testPropAnnotation: Unit = {
    val conf =
      """
        |# hello world
        |   # hello world
        | # 注释
        |   # 注释
        |#fire framework
        |#  fire   framework
        |      #fire framework
        | hive.cluster=batch
        |kafka.brokers1 = test#$kafka
        |kafka.brokers2=test # 注释kafka
        |#kafka.brokers3=test # kafka
        |""".stripMargin
    val normalValue = RegularUtils.propAnnotation.replaceAllIn(conf, "").replaceAll("\\|", "").trim
    println(normalValue)
    val valueProps = new Properties()
    val stringReader = new StringReader(normalValue)
    valueProps.load(stringReader)
    stringReader.close()
    assert(valueProps.size() == 3)
    assert(valueProps.getProperty("kafka.brokers1").equals("test#$kafka"))
    assert(valueProps.getProperty("kafka.brokers2").equals("test"))
  }
}
