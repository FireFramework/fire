package com.zto.fire.examples.flink.connector

import com.zto.fire.predef._
import org.apache.flink.configuration.ReadableConfig
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.sink.{DynamicTableSink, SinkFunctionProvider}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.types.DataType

/**
 * sql connector的sink
 * @author ChengLong 2021-5-7 15:48:03
 */
class BeanDynamicTableSink(tableSchema: TableSchema, options: ReadableConfig, dataType: DataType) extends DynamicTableSink {
  override def getChangelogMode(requestedMode: ChangelogMode): ChangelogMode = ChangelogMode.insertOnly()

  override def copy(): DynamicTableSink = new BeanDynamicTableSink(tableSchema, options, dataType)

  override def asSummaryString(): JString = "bean-sink"

  /**
   * 核心逻辑，定义如何将数据sink
   */
  override def getSinkRuntimeProvider(context: DynamicTableSink.Context): DynamicTableSink.SinkRuntimeProvider = {
    SinkFunctionProvider.of(new RichSinkFunction[RowData] {
      override def invoke(value: RowData, context: SinkFunction.Context): Unit = {
        println("sink---> " + value.toString)
      }
    })
  }
}