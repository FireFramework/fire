package com.zto.fire.examples.flink.connector.rocketmq

import com.zto.fire._
import com.zto.fire.examples.flink.connector.rocketmq.RocketMQOptions._
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.configuration.{ConfigOption, ReadableConfig}
import org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.ValueFieldsStrategy
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.connector.format.DecodingFormat
import org.apache.flink.table.connector.source.DynamicTableSource
import org.apache.flink.table.data.RowData
import org.apache.flink.table.factories.{DeserializationFormatFactory, DynamicTableFactory, DynamicTableSourceFactory, FactoryUtil}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.LogicalTypeRoot
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot
import org.apache.flink.util.Preconditions

import java.util
import java.util.Properties
import java.util.stream.IntStream

/**
 * sql connector的source与sink创建工厂
 *
 * @author ChengLong 2021-5-7 15:48:03
 */
class RocketMQDynamicTableFactory extends DynamicTableSourceFactory {
  val IDENTIFIER = "rocketmq"

  private def getValueDecodingFormat(helper: FactoryUtil.TableFactoryHelper): DecodingFormat[DeserializationSchema[RowData]] = {
    /*helper.discoverOptionalDecodingFormat(classOf[DeserializationFormatFactory], FactoryUtil.FORMAT)
      .orElseGet(() => helper.discoverDecodingFormat(classOf[DeserializationFormatFactory], FactoryUtil.FORMAT))*/
    helper.discoverDecodingFormat(classOf[DeserializationFormatFactory], FactoryUtil.FORMAT)
  }

  private def hasRocketMQClientProperties(tableOptions: util.Map[String, String]) = tableOptions
    .keySet
    .stream
    .anyMatch((k: String) => k.startsWith(RocketMQOptions.PROPERTIES_PREFIX))

  def getRocketMQProperties(tableOptions: util.Map[String, String]): Properties = {
    val rocketMQProperties = new Properties
    if (hasRocketMQClientProperties(tableOptions)) tableOptions.keySet.stream.filter((key: String) => key.startsWith(RocketMQOptions.PROPERTIES_PREFIX)).forEach((key: String) => {
      def foo(key: String) = {
        val value = tableOptions.get(key)
        val subKey = key.substring(RocketMQOptions.PROPERTIES_PREFIX.length)
        rocketMQProperties.put(subKey, value)
      }

      foo(key)
    })
    rocketMQProperties
  }

  def createKeyFormatProjection(options: ReadableConfig, physicalDataType: DataType): Array[Int] = {
    val physicalType = physicalDataType.getLogicalType

    val optionalKeyFormat = options.getOptional(RocketMQOptions.KEY_FORMAT)
    val optionalKeyFields = options.getOptional(RocketMQOptions.KEY_FIELDS)
    if (!optionalKeyFormat.isPresent && optionalKeyFields.isPresent) throw new ValidationException(String.format("The option '%s' can only be declared if a key format is defined using '%s'.", RocketMQOptions.KEY_FIELDS.key, RocketMQOptions.KEY_FORMAT.key))
    else if (optionalKeyFormat.isPresent && (!optionalKeyFields.isPresent || optionalKeyFields.get.size == 0)) throw new ValidationException(String.format("A key format '%s' requires the declaration of one or more of key fields using '%s'.", RocketMQOptions.KEY_FORMAT.key, RocketMQOptions.KEY_FIELDS.key))
    if (!optionalKeyFormat.isPresent) return new Array[Int](0)
    val keyPrefix = options.getOptional(RocketMQOptions.KEY_FIELDS_PREFIX).orElse("")
    val keyFields = optionalKeyFields.get
    val physicalFields = LogicalTypeChecks.getFieldNames(physicalType)
    keyFields.stream.mapToInt((keyField: String) => {
      def foo(keyField: String) = {
        val pos = physicalFields.indexOf(keyField)
        // check that field name exists
        if (pos < 0) throw new ValidationException(String.format("Could not find the field '%s' in the table schema for usage in the key format. " + "A key field must be a regular, physical column. " + "The following columns can be selected in the '%s' option:\n" + "%s", keyField, RocketMQOptions.KEY_FIELDS.key, physicalFields))
        // check that field name is prefixed correctly
        if (!keyField.startsWith(keyPrefix)) throw new ValidationException(String.format("All fields in '%s' must be prefixed with '%s' when option '%s' " + "is set but field '%s' is not prefixed.", RocketMQOptions.KEY_FIELDS.key, keyPrefix, RocketMQOptions.KEY_FIELDS_PREFIX.key, keyField))
        pos
      }

      foo(keyField)
    }).toArray
  }

  def createValueFormatProjection(options: ReadableConfig, physicalDataType: DataType): Array[Int] = {
    val physicalType = physicalDataType.getLogicalType

    val physicalFieldCount = LogicalTypeChecks.getFieldCount(physicalType)
    val physicalFields = IntStream.range(0, physicalFieldCount)

    val keyPrefix = options.getOptional(KEY_FIELDS_PREFIX).orElse("")

    val strategy = options.get(VALUE_FIELDS_INCLUDE);
    if (strategy == ValueFieldsStrategy.ALL) {
      if (keyPrefix.length() > 0) {
        throw new ValidationException(
          String.format(
            "A key prefix is not allowed when option '%s' is set to '%s'. "
              + "Set it to '%s' instead to avoid field overlaps.",
            VALUE_FIELDS_INCLUDE.key(),
            ValueFieldsStrategy.ALL,
            ValueFieldsStrategy.EXCEPT_KEY));
      }
      return physicalFields.toArray()
    } else if (strategy == ValueFieldsStrategy.EXCEPT_KEY) {
      val keyProjection = createKeyFormatProjection(options, physicalDataType);
      return physicalFields
        .filter(pos => IntStream.of(keyProjection: _*).noneMatch(k => k == pos))
        .toArray
    }
    throw new TableException("Unknown value fields strategy:" + strategy);
  }

  /**
   * 告诉工厂，如何创建Table Source实例
   */
  override def createDynamicTableSource(context: DynamicTableFactory.Context): DynamicTableSource = {
    val helper = FactoryUtil.createTableFactoryHelper(this, context)

    val tableOptions = helper.getOptions

    // val keyDecodingFormat: Optional[DecodingFormat[DeserializationSchema[RowData]]] = getKeyDecodingFormat(helper)

    val valueDecodingFormat = getValueDecodingFormat(helper)

    // val startupOptions = getStartupOptions(tableOptions)

    val properties = getRocketMQProperties(context.getCatalogTable.getOptions)

    val physicalDataType = context.getCatalogTable.getSchema.toPhysicalRowDataType

    val keyProjection = createKeyFormatProjection(tableOptions, physicalDataType);

    val valueProjection = createValueFormatProjection(tableOptions, physicalDataType);

    new RocketMQDynamicTableSource(physicalDataType,
      valueDecodingFormat,
      keyProjection,
      valueProjection,
      tableOptions.getOptional(RocketMQOptions.TOPIC).orElse(""),
      properties)
  }

  override def factoryIdentifier(): String = this.IDENTIFIER

  /**
   * 必填参数列表
   */
  override def requiredOptions(): JSet[ConfigOption[_]] = {
    val set = new JHashSet[ConfigOption[_]]
    set.add(RocketMQOptions.TOPIC)
    set.add(RocketMQOptions.PROPS_BOOTSTRAP_SERVERS)
    set.add(RocketMQOptions.PROPS_GROUP_ID)
    set
  }

  /**
   * 可选的参数列表
   */
  override def optionalOptions(): JSet[ConfigOption[_]] = {
    val optionalOptions = new JHashSet[ConfigOption[_]]
    optionalOptions.add(RocketMQOptions.SCAN_STARTUP_MODE)
    optionalOptions.add(RocketMQOptions.SCAN_STARTUP_SPECIFIC_OFFSETS)
    optionalOptions.add(RocketMQOptions.TOPIC_PATTERN)
    optionalOptions
  }
}
