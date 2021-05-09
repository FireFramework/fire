package com.zto.fire.examples.flink.connector.rocketmq

import com.zto.fire._
import org.apache.flink.configuration.{ConfigOption, ConfigOptions, ReadableConfig}
import org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.ValueFieldsStrategy

/**
 * 自定义sql connector支持的选项
 *
 * @author ChengLong 2021-5-7 15:48:03
 */
object RocketMQOptions {
  val PROPERTIES_PREFIX = "properties."

  val TOPIC: ConfigOption[String] = ConfigOptions
    .key("topic")
    .stringType
    .noDefaultValue
    .withDescription("Topic names from which the table is read. Either 'topic' or 'topic-pattern' must be set for source. " + "Option 'topic' is required for sink.")

  val TOPIC_PATTERN: ConfigOption[String] = ConfigOptions
    .key("topic-pattern")
    .stringType
    .noDefaultValue
    .withDescription("Optional topic pattern from which the table is read for source. Either 'topic' or 'topic-pattern' must be set.")

  val PROPS_BOOTSTRAP_SERVERS: ConfigOption[String] = ConfigOptions
    .key("properties.bootstrap.servers")
    .stringType
    .noDefaultValue
    .withDescription("Required RocketMQ server connection string")

  val PROPS_GROUP_ID: ConfigOption[String] = ConfigOptions
    .key("properties.group.id")
    .stringType.noDefaultValue
    .withDescription("Required consumer group in RocketMQ consumer, no need for v producer")

  val SCAN_STARTUP_MODE: ConfigOption[String] = ConfigOptions
    .key("scan.startup.mode")
    .stringType
    .defaultValue("group-offsets")
    .withDescription("Optional startup mode for RocketMQ consumer, valid enumerations are " + "\"earliest-offset\", \"latest-offset\", \"group-offsets\", \"timestamp\"\n" + "or \"specific-offsets\"")

  val SCAN_STARTUP_SPECIFIC_OFFSETS: ConfigOption[String] = ConfigOptions
    .key("scan.startup.specific-offsets")
    .stringType
    .noDefaultValue
    .withDescription("Optional offsets used in case of \"specific-offsets\" startup mode")


  val KEY_FIELDS_PREFIX: ConfigOption[String] =
    ConfigOptions.key("key.fields-prefix")
      .stringType()
      .noDefaultValue()
      .withDescription(
        "Defines a custom prefix for all fields of the key format to avoid "
          + "name clashes with fields of the value format. By default, the prefix is empty. "
          + "If a custom prefix is defined, both the table schema and "
          + "'"
          + "' will work with prefixed names. When constructing "
          + "the data type of the key format, the prefix will be removed and the "
          + "non-prefixed names will be used within the key format. Please note that this "
          + "option requires that '"
          + "' must be '"
          + ValueFieldsStrategy.EXCEPT_KEY
          + "'.")

  val KEY_FIELDS: ConfigOption[JList[String]] =
    ConfigOptions.key("key.fields")
      .stringType()
      .asList()
      .defaultValues()
      .withDescription(
        "Defines an explicit list of physical columns from the table schema "
          + "that configure the data type for the key format. By default, this list is "
          + "empty and thus a key is undefined.")

  val VALUE_FIELDS_INCLUDE: ConfigOption[ValueFieldsStrategy] =
    ConfigOptions.key("value.fields-include")
      .enumType(classOf[ValueFieldsStrategy])
      .defaultValue(ValueFieldsStrategy.ALL)
      .withDescription(
        "Defines a strategy how to deal with key columns in the data type of "
          + "the value format. By default, '"
          + ValueFieldsStrategy.ALL
          + "' physical "
          + "columns of the table schema will be included in the value format which "
          + "means that key columns appear in the data type for both the key and value "
          + "format.")

  val FORMAT_SUFFIX = ".format"

  val KEY_FORMAT: ConfigOption[String] =
    ConfigOptions.key("key" + FORMAT_SUFFIX)
      .stringType()
      .noDefaultValue()
      .withDescription(
        "Defines the format identifier for encoding key data. "
          + "The identifier is used to discover a suitable format factory.")

  val VALUE_FORMAT: ConfigOption[String] =
    ConfigOptions.key("value" + FORMAT_SUFFIX)
      .stringType()
      .noDefaultValue()
      .withDescription(
        "Defines the format identifier for encoding value data. "
          + "The identifier is used to discover a suitable format factory.")

}
