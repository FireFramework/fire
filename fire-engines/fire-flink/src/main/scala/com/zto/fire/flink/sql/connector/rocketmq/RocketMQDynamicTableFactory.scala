package com.zto.fire.flink.sql.connector.rocketmq

import com.zto.fire._
import com.zto.fire.common.conf.FireRocketMQConf
import com.zto.fire.flink.sql.connector.rocketmq.RocketMQOptions._
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
    val withOptions = context.getCatalogTable.getOptions
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
      withOptions)
  }
}
