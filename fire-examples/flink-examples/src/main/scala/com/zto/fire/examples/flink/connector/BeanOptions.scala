package com.zto.fire.examples.flink.connector

import com.zto.fire._
import org.apache.flink.configuration.{ConfigOption, ConfigOptions}

/**
 * 自定义sql connector支持的选项
 *
 * @author ChengLong 2021-5-7 15:48:03
 */
object BeanOptions {
  val TABLE_NAME: ConfigOption[String] = ConfigOptions
    .key("table-name")
    .stringType
    .noDefaultValue
    .withDescription("The name of impala table to connect.")

  val DURATION: ConfigOption[JLong] = ConfigOptions
    .key("duration")
    .longType()
    .defaultValue(3000L)
    .withDescription("The duration of data send.")

  val repeatTimes: ConfigOption[JInt] = ConfigOptions
    .key("repeat-times")
    .intType()
    .defaultValue(5)
    .withDescription("The repeat times.")
}
