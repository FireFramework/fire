package com.zto.fire.flink.sql.connector.rocketmq

import com.zto.fire.predef._
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.format.DecodingFormat
import org.apache.flink.table.connector.source.{DynamicTableSource, ScanTableSource, SourceFunctionProvider}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.utils.DataTypeUtils
import org.apache.rocketmq.flink.serialization.JsonDeserializationSchema
import org.apache.rocketmq.flink.{RocketMQConfig, RocketMQSource}

import java.util.Properties

/**
 * 定义source table
 *
 * @author ChengLong 2021-5-7 15:48:03
 */
class RocketMQDynamicTableSource(physicalDataType: DataType,
                                 keyDecodingFormat: DecodingFormat[DeserializationSchema[RowData]],
                                 valueDecodingFormat: DecodingFormat[DeserializationSchema[RowData]],
                                 keyProjection: Array[Int],
                                 valueProjection: Array[Int],
                                 keyPrefix: String,
                                 topic: String,
                                 properties: Properties) extends ScanTableSource {

  override def getChangelogMode: ChangelogMode = ChangelogMode.insertOnly()

  override def copy(): DynamicTableSource = new RocketMQDynamicTableSource(physicalDataType, keyDecodingFormat, valueDecodingFormat, keyProjection, valueProjection, keyPrefix, topic, properties)

  override def asSummaryString(): String = "fire-rocketmq"

  /**
   * 创建反序列化器
   */
  def createDeserialization(context: DynamicTableSource.Context, format: DecodingFormat[DeserializationSchema[RowData]], projection: Array[Int], prefix: String): DeserializationSchema[RowData] = {
    if (format == null) return null

    var physicalFormatDataType = DataTypeUtils.projectRow(this.physicalDataType, projection)
    if (noEmpty(prefix)) {
      physicalFormatDataType = DataTypeUtils.stripRowPrefix(physicalFormatDataType, prefix)
    }
    format.createRuntimeDecoder(context, physicalFormatDataType)
  }

  /**
   * 消费rocketmq中的数据，并反序列化为RowData对象实例
   */
  override def getScanRuntimeProvider(context: ScanTableSource.ScanContext): ScanTableSource.ScanRuntimeProvider = {
    val keyDeserialization = createDeserialization(context, keyDecodingFormat, keyProjection, keyPrefix)
    val valueDeserialization = createDeserialization(context, valueDecodingFormat, valueProjection, null)
    properties.setProperty(RocketMQConfig.CONSUMER_TOPIC, topic)
    SourceFunctionProvider.of(new RocketMQSource(new JsonDeserializationSchema(keyDeserialization, valueDeserialization), properties), false)
  }

}
