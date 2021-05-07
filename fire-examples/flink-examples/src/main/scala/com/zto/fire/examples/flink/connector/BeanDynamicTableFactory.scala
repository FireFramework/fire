package com.zto.fire.examples.flink.connector

import com.zto.fire._
import org.apache.flink.configuration.ConfigOption
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.table.connector.source.DynamicTableSource
import org.apache.flink.table.factories.{DynamicTableFactory, DynamicTableSinkFactory, DynamicTableSourceFactory, FactoryUtil}
import org.apache.flink.table.utils.TableSchemaUtils

/**
 * sql connector的source与sink创建工厂
 *
 * @author ChengLong 2021-5-7 15:48:03
 */
class BeanDynamicTableFactory extends DynamicTableSourceFactory with DynamicTableSinkFactory {
  val IDENTIFIER = "bean"

  /**
   * 告诉工厂，如何创建Table Source实例
   */
  override def createDynamicTableSource(context: DynamicTableFactory.Context): DynamicTableSource = {
    val helper = FactoryUtil.createTableFactoryHelper(this, context)
    val config = helper.getOptions
    helper.validate()

    val physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable.getSchema)
    new BeanDynamicTableSource(physicalSchema,
      config,
      physicalSchema.toRowDataType)
  }

  override def factoryIdentifier(): String = this.IDENTIFIER

  /**
   * 必填参数列表
   */
  override def requiredOptions(): JSet[ConfigOption[_]] = {
    val set = new JHashSet[ConfigOption[_]]
    set.add(BeanOptions.TABLE_NAME)
    set
  }

  /**
   * 可选的参数列表
   */
  override def optionalOptions(): JSet[ConfigOption[_]] = {
    val optionalOptions = new JHashSet[ConfigOption[_]]
    optionalOptions.add(BeanOptions.DURATION)
    optionalOptions.add(BeanOptions.repeatTimes)
    optionalOptions
  }

  /**
   * 创建table sink实例，在BeanDynamicTableSink中定义接收到的RowData如何sink
   */
  override def createDynamicTableSink(context: DynamicTableFactory.Context): DynamicTableSink = {
    val helper = FactoryUtil.createTableFactoryHelper(this, context)
    val physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable.getSchema)
    val config = helper.getOptions
    helper.validate()
    val dataType = context.getCatalogTable.getSchema.toPhysicalRowDataType
    new BeanDynamicTableSink(physicalSchema, config, dataType)
  }
}
