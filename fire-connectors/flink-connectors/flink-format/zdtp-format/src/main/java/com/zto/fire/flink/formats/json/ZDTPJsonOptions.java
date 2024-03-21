package com.zto.fire.flink.formats.json;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.formats.json.JsonFormatOptions;


public class ZDTPJsonOptions {
    public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS = JsonFormatOptions.IGNORE_PARSE_ERRORS;

    public static final ConfigOption<String> TIMESTAMP_FORMAT = JsonFormatOptions.TIMESTAMP_FORMAT;

    public static final ConfigOption<String> JSON_MAP_NULL_KEY_MODE = JsonFormatOptions.MAP_NULL_KEY_MODE;

    public static final ConfigOption<String> JSON_MAP_NULL_KEY_LITERAL = JsonFormatOptions.MAP_NULL_KEY_LITERAL;

    public static final ConfigOption<Boolean> CDC_WRITE_HIVE =
            ConfigOptions.key("cdc-write-hive")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "原生的maxwell-json canal-json debezium-json 因为其getChangelogMode " +
                                    "返回值为ChangelogMode.all()，当下游是hive时，flink框架会判断报错，因为hive" +
                                    "只支持insert模式，添加这个参数主要针对binlog写hive的场景。");
}
