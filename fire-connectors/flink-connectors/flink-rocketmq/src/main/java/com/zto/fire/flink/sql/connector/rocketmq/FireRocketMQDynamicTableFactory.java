package com.zto.fire.flink.sql.connector.rocketmq;

import com.zto.fire.common.enu.Operation;
import com.zto.fire.common.lineage.parser.connector.MQDatasource;
import com.zto.fire.common.lineage.parser.connector.RocketmqConnectorParser;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.Format;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.rocketmq.flink.RocketMQConfig;
import scala.collection.JavaConverters;

import java.util.*;

import static com.zto.fire.common.enu.Datasource.ROCKETMQ;
import static com.zto.fire.flink.sql.connector.rocketmq.FireRocketMQOptions.*;

/**
 * 描述：本类通过java SPI机制，对flink 的 table connector 进行扩展，
 * 配置在source/META-INF/services/org.apache.flink.table.table.factories.Factory中，
 * create table rocket_mq_table(
 * .......
 * ) WITH (
 * 'connector' = 'rocketmq',
 * ......
 * )
 * 时间：19/8/2021 下午5:18
 */
public class FireRocketMQDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "rocketmq";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig tableOptions = helper.getOptions();

        // 通过 key.format 找到针对 MQ key 的解析格式，如 'key.format' = 'json'
        Optional<DecodingFormat<DeserializationSchema<RowData>>> keyDecodingFormat =
                getKeyDecodingFormat(helper);

        final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
                getValueDecodingFormat(helper);

        // 验证主键和value的模式是否冲突
        validatePKConstraints(context.getObjectIdentifier(), context.getCatalogTable(), valueDecodingFormat);

        // ROW<`k_age` INT, `k_kk` STRING, `id` STRING, `name` STRING, `description` STRING, `weight` FLOAT> NOT NULL
        final DataType physicalDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();

        final int[] keyProjection = createKeyFormatProjection(tableOptions, physicalDataType);

        final int[] valueProjection = createValueFormatProjection(tableOptions, physicalDataType);

        final String keyPrefix = tableOptions.getOptional(KEY_FIELDS_PREFIX).orElse(null);

        Properties properties = getRocketMQStartupProperties(tableOptions);

        return new FireRocketMQDynamicSource(
                physicalDataType,
                keyDecodingFormat.orElse(null),
                valueDecodingFormat,
                keyProjection,
                valueProjection,
                keyPrefix,
                properties,
                false);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(NAME_SERVER);
        options.add(TOPIC);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SCAN_START_MODE);
        options.add(CONSUMER_GROUP);
        options.add(FactoryUtil.FORMAT);
        options.add(VALUE_FORMAT);
        options.add(VALUE_FIELDS_INCLUDE);
        options.add(KEY_FORMAT);
        options.add(KEY_FIELDS_PREFIX);
        options.add(KEY_FIELDS);
        options.add(TAGS);
        return options;
    }

    // rocket mq consumer 需要的参数
    private static Properties getRocketMQStartupProperties(ReadableConfig tableOptions) {
        String nameServers = tableOptions.get(NAME_SERVER);
        String topic = tableOptions.get(TOPIC);
        String consumerGroup = tableOptions.get(CONSUMER_GROUP);

        Set<Operation> set = new HashSet<>();
        set.add(Operation.SOURCE);

        MQDatasource mqDatasource = new MQDatasource("rocketmq", nameServers, topic, consumerGroup, set);
        // 消费rocketmq埋点信息
        RocketmqConnectorParser.addDatasource(ROCKETMQ, mqDatasource);

        String scanStartMode = tableOptions.get(SCAN_START_MODE);
        Properties properties = new Properties();
        properties.setProperty(RocketMQConfig.NAME_SERVER_ADDR, nameServers);
        properties.setProperty(RocketMQConfig.CONSUMER_TOPIC, topic);
        properties.setProperty(RocketMQConfig.CONSUMER_GROUP, consumerGroup);
        properties.setProperty(RocketMQConfig.CONSUMER_OFFSET_RESET_TO, scanStartMode);
        String tags = tableOptions.get(TAGS);
        if (tags != null)
            properties.setProperty(RocketMQConfig.CONSUMER_TAG, tags);
        return properties;
    }

    private static Optional<DecodingFormat<DeserializationSchema<RowData>>> getKeyDecodingFormat(
            FactoryUtil.TableFactoryHelper helper) {
        final Optional<DecodingFormat<DeserializationSchema<RowData>>> keyDecodingFormat =
                helper.discoverOptionalDecodingFormat(
                        DeserializationFormatFactory.class, KEY_FORMAT);
        keyDecodingFormat.ifPresent(
                format -> {
                    if (!format.getChangelogMode().containsOnly(RowKind.INSERT)) {
                        throw new ValidationException(
                                String.format(
                                        "A key format should only deal with INSERT-only records. "
                                                + "But %s has a changelog mode of %s.",
                                        helper.getOptions().get(KEY_FORMAT),
                                        format.getChangelogMode()));
                    }
                });
        return keyDecodingFormat;
    }

    // 'format' = 'zdtp-json' or 'value.format' = 'zdtp-json'
    private static DecodingFormat<DeserializationSchema<RowData>> getValueDecodingFormat(
            FactoryUtil.TableFactoryHelper helper) {
        return helper.discoverOptionalDecodingFormat(DeserializationFormatFactory.class, FactoryUtil.FORMAT)
                .orElseGet(
                        () -> helper.discoverDecodingFormat(DeserializationFormatFactory.class, VALUE_FORMAT));

    }

    private static void validatePKConstraints(
            ObjectIdentifier tableName, CatalogTable catalogTable, Format format) {
        if (catalogTable.getSchema().getPrimaryKey().isPresent()
                && format.getChangelogMode().containsOnly(RowKind.INSERT)) {
            Configuration options = Configuration.fromMap(catalogTable.getOptions());
            String formatName =
                    options.getOptional(FactoryUtil.FORMAT).orElse(options.get(VALUE_FORMAT));
            throw new ValidationException(
                    String.format(
                            "The Kafka table '%s' with '%s' format doesn't support defining PRIMARY KEY constraint"
                                    + " on the table, because it can't guarantee the semantic of primary key.",
                            tableName.asSummaryString(), formatName));
        }
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
//        return new RocketMQDynamicSink();
        return null;
    }
}
