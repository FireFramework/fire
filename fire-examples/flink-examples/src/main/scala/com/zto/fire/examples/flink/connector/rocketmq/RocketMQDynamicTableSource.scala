package com.zto.fire.examples.flink.connector.rocketmq

import com.zto.fire.predef._
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.format.DecodingFormat
import org.apache.flink.table.connector.source.{DynamicTableSource, ScanTableSource, SourceFunctionProvider}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.utils.DataTypeUtils
import org.apache.rocketmq.client.consumer.listener.{ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, MessageListenerConcurrently}
import org.apache.rocketmq.client.consumer.{DefaultMQPullConsumer, DefaultMQPushConsumer, PullStatus}
import org.apache.rocketmq.common.consumer.ConsumeFromWhere
import org.apache.rocketmq.common.message.{MessageExt, MessageQueue}

import java.util
import java.util.Properties

/**
 * 定义source table
 *
 * @author ChengLong 2021-5-7 15:48:03
 */
class RocketMQDynamicTableSource(physicalDataType: DataType,
                                 valueDecodingFormat: DecodingFormat[DeserializationSchema[RowData]],
                                 keyProjection: Array[Int],
                                 valueProjection: Array[Int],
                                 topics: util.List[String],
                                 properties: Properties) extends ScanTableSource {

  override def getChangelogMode: ChangelogMode = ChangelogMode.insertOnly()

  override def copy(): DynamicTableSource = new RocketMQDynamicTableSource(physicalDataType, valueDecodingFormat, keyProjection, valueProjection, topics, properties)

  override def asSummaryString(): String = "rocketmq"

  def createDeserialization(context: DynamicTableSource.Context, format: DecodingFormat[DeserializationSchema[RowData]], projection: Array[Int], prefix: String): DeserializationSchema[RowData] = {
    if (format == null) {
      return null
    }

    var physicalFormatDataType = DataTypeUtils.projectRow(this.physicalDataType, projection)
    if (noEmpty(prefix)) {
      physicalFormatDataType = DataTypeUtils.stripRowPrefix(physicalFormatDataType, prefix)
    }
    format.createRuntimeDecoder(context, physicalFormatDataType)
  }

  /**
   * 核心逻辑，定义如何产生source表的数据
   */
  override def getScanRuntimeProvider(context: ScanTableSource.ScanContext): ScanTableSource.ScanRuntimeProvider = {
    // val keyDeserialization = createDeserialization(context, keyDecodingFormat, keyProjection, keyPrefix);
    val valueDeserialization = createDeserialization(context, valueDecodingFormat, valueProjection, null);
    val producedTypeInfo: TypeInformation[RowData] = context.createTypeInformation(physicalDataType)
    SourceFunctionProvider.of(new RocketMQSourceFunction(physicalDataType, producedTypeInfo, valueDeserialization, topics, properties), false)
  }

}

/**
 * 自定义的sink function，用于通知flink sql，如何将RowData数据收集起来
 */
class RocketMQSourceFunction(physicalDataType: DataType,
                             producedTypeInfo: TypeInformation[RowData],
                             valueDeserialization: DeserializationSchema[RowData],
                             topics: util.List[String],
                             properties: Properties) extends RichSourceFunction[RowData] {
  val offsetTable = new JHashMap[MessageQueue, Long]()

  def putMessageQueueOffset(mq: MessageQueue, offset: Long): Unit = {
    offsetTable.put(mq, offset)
  }

  def getMessageQueueOffset(mq: MessageQueue): Long = {
    val offset = offsetTable.get(mq)
    if (offset != null) offset else 0
  }


  override def run(ctx: SourceFunction.SourceContext[RowData]): Unit = {

    val consumer = new DefaultMQPullConsumer(properties.getProperty("group.id"))
    consumer.setNamesrvAddr(properties.getProperty("bootstrap.servers"))
    consumer.start()

    val mqs = consumer.fetchSubscribeMessageQueues(topics.get(0))
    mqs.foreach(mq => {
      val pullResult = consumer.pullBlockIfNotFound(mq, null, getMessageQueueOffset(mq), 21)
      putMessageQueueOffset(mq, pullResult.getNextBeginOffset)
      pullResult.getPullStatus match {
        case PullStatus.FOUND => {
          val msgList = pullResult.getMsgFoundList
          msgList.foreach(msg => {
            val data = valueDeserialization.deserialize(new String(msg.getBody).getBytes())
            ctx.collect(data)
          })
        }
        case _ => println("--------> ")
      }

    })
  }


  override def cancel(): Unit = {}

}