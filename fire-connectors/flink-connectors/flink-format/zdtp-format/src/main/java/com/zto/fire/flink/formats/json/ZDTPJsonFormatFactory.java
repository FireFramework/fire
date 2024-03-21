package com.zto.fire.flink.formats.json;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptionsUtil;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.zto.fire.flink.formats.json.ZDTPJsonOptions.*;


/**
 * 描述：flink 官网只提供了 canal maxwell debezium三种CDC connector,
 * 由于目前使用的是中通自己开发的ZDTP 组件采集的binlog,
 * 所以要写一个关于ZDTP组件来解析CDC消息，
 * create table rocket_mq_table(
 * `bill_code` string,
 * `row_kind` STRING METADATA FROM 'value.row_kind' VIRTUAL, // 从binlog中提取的rowKind
 * `mq_topic` STRING METADATA FROM 'topic' VIRTUAL, // topic
 * `mq_broker` STRING METADATA FROM 'broker' VIRTUAL, // broker
 * `id` bigint,
 * customize_id string
 * ) WITH (
 * 'connector' = '...',
 * 'format' = 'zdtp-json',
 * 'zdtp-json.cdc-write-hive' = 'true', // 添加此参数，下游可以直接insert 到hive
 * ......
 * )
 */
public class ZDTPJsonFormatFactory implements DeserializationFormatFactory {
    private static final String IDENTIFIER = "zdtp-json";
    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        final boolean ignoreParseErrors = formatOptions.get(IGNORE_PARSE_ERRORS);
        final boolean cdcWriteHive = formatOptions.get(CDC_WRITE_HIVE);
        final TimestampFormat timestampFormatOption = JsonFormatOptionsUtil.getTimestampFormat(formatOptions);

        return new ZDTPJsonDecodingFormat(ignoreParseErrors, cdcWriteHive, timestampFormatOption);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(IGNORE_PARSE_ERRORS);
        options.add(TIMESTAMP_FORMAT);
        options.add(JSON_MAP_NULL_KEY_MODE);
        options.add(JSON_MAP_NULL_KEY_LITERAL);
        return options;
    }
}
