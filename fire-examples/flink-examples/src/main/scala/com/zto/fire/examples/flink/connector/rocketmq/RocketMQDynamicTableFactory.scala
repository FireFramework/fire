package com.zto.fire.examples.flink.connector.rocketmq

import com.zto.fire._
import com.zto.fire.examples.flink.connector.rocketmq.RocketMQOptions._
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.configuration.ConfigOption
import org.apache.flink.table.connector.format.DecodingFormat
import org.apache.flink.table.connector.source.DynamicTableSource
import org.apache.flink.table.data.RowData
import org.apache.flink.table.factories.{DeserializationFormatFactory, DynamicTableFactory, DynamicTableSourceFactory, FactoryUtil}

import java.util
import java.util.Properties

/**
 * sql connector的source与sink创建工厂
 *
 * @author ChengLong 2021-5-7 15:48:03
 */
class RocketMQDynamicTableFactory extends DynamicTableSourceFactory {
  val IDENTIFIER = "fire-rocketmq"

  override def factoryIdentifier(): String = this.IDENTIFIER

  private def getValueDecodingFormat(helper: FactoryUtil.TableFactoryHelper): DecodingFormat[DeserializationSchema[RowData]] = {
    helper.discoverDecodingFormat(classOf[DeserializationFormatFactory], FactoryUtil.FORMAT)
  }

  private def getKeyDecodingFormat(helper: FactoryUtil.TableFactoryHelper): DecodingFormat[DeserializationSchema[RowData]] = {
    helper.discoverDecodingFormat(classOf[DeserializationFormatFactory], FactoryUtil.FORMAT)
  }

  /**
   * 是否存在以properties.开头的参数
   */
  private def hasRocketMQClientProperties(tableOptions: util.Map[String, String]) = tableOptions
    .keySet
    .stream
    .anyMatch((k: String) => k.startsWith(PROPERTIES_PREFIX))

  /**
   * 获取以properties.开头的所有的参数
   */
  def getRocketMQProperties(tableOptions: util.Map[String, String]): Properties = {
    val rocketMQProperties = new Properties
    if (hasRocketMQClientProperties(tableOptions)) tableOptions.keySet.stream.filter((key: String) => key.startsWith(PROPERTIES_PREFIX)).forEach((key: String) => {
      def foo(key: String): Unit = {
        val value = tableOptions.get(key)
        val subKey = key.substring(PROPERTIES_PREFIX.length)
        rocketMQProperties.put(subKey, value)
      }

      foo(key)
    })
    rocketMQProperties
  }

  /**
   * 必填参数列表
   */
  override def requiredOptions(): JSet[ConfigOption[_]] = {
    val set = new JHashSet[ConfigOption[_]]
    set.add(TOPIC)
    set.add(PROPS_BOOTSTRAP_SERVERS)
    set.add(PROPS_GROUP_ID)
    set
  }

  /**
   * 可选的参数列表
   */
  override def optionalOptions(): JSet[ConfigOption[_]] = {
    val optionalOptions = new JHashSet[ConfigOption[_]]
    optionalOptions
  }


  /**
   * 创建rocketmq table source
   */
  override def createDynamicTableSource(context: DynamicTableFactory.Context): DynamicTableSource = {
    val helper = FactoryUtil.createTableFactoryHelper(this, context)

    val tableOptions = helper.getOptions
    val keyDecodingFormat = this.getKeyDecodingFormat(helper)
    val valueDecodingFormat = this.getValueDecodingFormat(helper)
    val properties = getRocketMQProperties(context.getCatalogTable.getOptions)
    val physicalDataType = context.getCatalogTable.getSchema.toPhysicalRowDataType
    val keyProjection = createKeyFormatProjection(tableOptions, physicalDataType)
    val valueProjection = createValueFormatProjection(tableOptions, physicalDataType)
    val keyPrefix = tableOptions.getOptional(KEY_FIELDS_PREFIX).orElse(null)

    new RocketMQDynamicTableSource(physicalDataType,
      keyDecodingFormat,
      valueDecodingFormat,
      keyProjection,
      valueProjection,
      keyPrefix,
      tableOptions.getOptional(TOPIC).orElse(""),
      properties)
  }
}
