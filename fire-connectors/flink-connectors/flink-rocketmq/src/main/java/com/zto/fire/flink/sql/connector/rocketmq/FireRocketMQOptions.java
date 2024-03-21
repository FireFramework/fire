package com.zto.fire.flink.sql.connector.rocketmq;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.apache.flink.table.factories.FactoryUtil.FORMAT_SUFFIX;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot;

public class FireRocketMQOptions {
    private FireRocketMQOptions(){}

    public static final String SCAN_STARTUP_MODE_VALUE_EARLIEST = "earliest-offset";
    public static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";

    public static final ConfigOption<String> SCAN_START_MODE =
            ConfigOptions.key("scan.startup.mode")
                    .stringType()
                    .defaultValue("latest-offset")
                    .withDescription("指定rocket mq 消费偏移量，目前只支持(earliest-offset) 和 (latest-offset)");


    public static final ConfigOption<String> NAME_SERVER =
            ConfigOptions.key("properties.bootstrap.servers")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("");


    public static final ConfigOption<String> TOPIC =
            ConfigOptions.key("topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("");

    public static final ConfigOption<String> CONSUMER_GROUP =
            ConfigOptions.key("properties.group.id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("");

    public static final ConfigOption<String> TAGS =
            ConfigOptions.key("tags")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("指定rocket mq 消费 tag,用逗号分隔");

    public static final ConfigOption<String> KEY_FORMAT =
            ConfigOptions.key("key" + FORMAT_SUFFIX)
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines the format identifier for encoding key data. "
                                    + "The identifier is used to discover a suitable format factory.");

    public static final ConfigOption<List<String>> KEY_FIELDS =
            ConfigOptions.key("key.fields")
                    .stringType()
                    .asList()
                    .defaultValues()
                    .withDescription(
                            "Defines an explicit list of physical columns from the table schema "
                                    + "that configure the data type for the key format. By default, this list is "
                                    + "empty and thus a key is undefined.");

    public static final ConfigOption<ValueFieldsStrategy> VALUE_FIELDS_INCLUDE =
            ConfigOptions.key("value.fields-include")
                    .enumType(ValueFieldsStrategy.class)
                    .defaultValue(ValueFieldsStrategy.ALL)
                    .withDescription(
                            "Defines a strategy how to deal with key columns in the data type of "
                                    + "the value format. By default, '"
                                    + ValueFieldsStrategy.ALL
                                    + "' physical "
                                    + "columns of the table schema will be included in the value format which "
                                    + "means that key columns appear in the data type for both the key and value "
                                    + "format.");

    public static final ConfigOption<String> VALUE_FORMAT =
            ConfigOptions.key("value" + FORMAT_SUFFIX)
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines the format identifier for encoding value data. "
                                    + "The identifier is used to discover a suitable format factory.");

    public static final ConfigOption<String> KEY_FIELDS_PREFIX =
            ConfigOptions.key("key.fields-prefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines a custom prefix for all fields of the key format to avoid "
                                    + "name clashes with fields of the value format. By default, the prefix is empty. "
                                    + "If a custom prefix is defined, both the table schema and "
                                    + "'"
                                    + KEY_FIELDS.key()
                                    + "' will work with prefixed names. When constructing "
                                    + "the data type of the key format, the prefix will be removed and the "
                                    + "non-prefixed names will be used within the key format. Please note that this "
                                    + "option requires that '"
                                    + VALUE_FIELDS_INCLUDE.key()
                                    + "' must be '"
                                    + ValueFieldsStrategy.EXCEPT_KEY
                                    + "'.");


//    public static StartupMode getStartupMode(ReadableConfig tableOptions) {
//        StartupMode startupMode = tableOptions.getOptional(SCAN_START_MODE)
//                .map(modeString -> {
//                    switch (modeString) {
//                        case (SCAN_STARTUP_MODE_VALUE_EARLIEST):
//                            return StartupMode.EARLIEST;
//                        case (SCAN_STARTUP_MODE_VALUE_LATEST):
//                            return StartupMode.LATEST;
//                        default:throw new TableException("Unsupported scan.start.mode:" + modeString);
//                    }
//                }).orElse(StartupMode.LATEST);
//        return startupMode;
//    }

    /**
     * Creates an array of indices that determine which physical fields of the table schema to
     * include in the key format and the order that those fields have in the key format.
     *
     * <p>See {@link #KEY_FORMAT}, {@link #KEY_FIELDS}, and {@link #KEY_FIELDS_PREFIX} for more
     * information.
     */
    public static int[] createKeyFormatProjection(
            ReadableConfig options, DataType physicalDataType) {
        final LogicalType physicalType = physicalDataType.getLogicalType();
        Preconditions.checkArgument(
                hasRoot(physicalType, LogicalTypeRoot.ROW), "Row data type expected.");
        final Optional<String> optionalKeyFormat = options.getOptional(KEY_FORMAT);
        final Optional<List<String>> optionalKeyFields = options.getOptional(KEY_FIELDS);// Optional[[k_age, k_kk]]

        if (!optionalKeyFormat.isPresent() && optionalKeyFields.isPresent()) {
            throw new ValidationException(
                    String.format(
                            "The option '%s' can only be declared if a key format is defined using '%s'.",
                            KEY_FIELDS.key(), KEY_FORMAT.key()));
        } else if (optionalKeyFormat.isPresent()
                && (!optionalKeyFields.isPresent() || optionalKeyFields.get().size() == 0)) {
            throw new ValidationException(
                    String.format(
                            "A key format '%s' requires the declaration of one or more of key fields using '%s'.",
                            KEY_FORMAT.key(), KEY_FIELDS.key()));
        }

        if (!optionalKeyFormat.isPresent()) {
            return new int[0];
        }

        final String keyPrefix = options.getOptional(KEY_FIELDS_PREFIX).orElse("");

        final List<String> keyFields = optionalKeyFields.get();
        final List<String> physicalFields = LogicalTypeChecks.getFieldNames(physicalType);
        return keyFields.stream()
                .mapToInt(
                        keyField -> {
                            final int pos = physicalFields.indexOf(keyField);
                            // check that field name exists
                            if (pos < 0) {
                                throw new ValidationException(
                                        String.format(
                                                "Could not find the field '%s' in the table schema for usage in the key format. "
                                                        + "A key field must be a regular, physical column. "
                                                        + "The following columns can be selected in the '%s' option:\n"
                                                        + "%s",
                                                keyField, KEY_FIELDS.key(), physicalFields));
                            }
                            // check that field name is prefixed correctly
                            if (!keyField.startsWith(keyPrefix)) {
                                throw new ValidationException(
                                        String.format(
                                                "All fields in '%s' must be prefixed with '%s' when option '%s' "
                                                        + "is set but field '%s' is not prefixed.",
                                                KEY_FIELDS.key(),
                                                keyPrefix,
                                                KEY_FIELDS_PREFIX.key(),
                                                keyField));
                            }
                            return pos;
                        })
                .toArray();
    }

    /**
     * Creates an array of indices that determine which physical fields of the table schema to
     * include in the value format.
     *
     * <p>See {@link #VALUE_FORMAT}, {@link #VALUE_FIELDS_INCLUDE}, and {@link #KEY_FIELDS_PREFIX}
     * for more information.
     */
    public static int[] createValueFormatProjection(
            ReadableConfig options, DataType physicalDataType) {
        final LogicalType physicalType = physicalDataType.getLogicalType();
        Preconditions.checkArgument(
                hasRoot(physicalType, LogicalTypeRoot.ROW), "Row data type expected.");
        final int physicalFieldCount = LogicalTypeChecks.getFieldCount(physicalType);
        final IntStream physicalFields = IntStream.range(0, physicalFieldCount);

        final String keyPrefix = options.getOptional(KEY_FIELDS_PREFIX).orElse("");

        final ValueFieldsStrategy strategy = options.get(VALUE_FIELDS_INCLUDE);
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
            return physicalFields.toArray();
        } else if (strategy == ValueFieldsStrategy.EXCEPT_KEY) {
            final int[] keyProjection = createKeyFormatProjection(options, physicalDataType);
            return physicalFields
                    .filter(pos -> IntStream.of(keyProjection).noneMatch(k -> k == pos))
                    .toArray();
        }
        throw new TableException("Unknown value fields strategy:" + strategy);
    }

    enum ValueFieldsStrategy{
        EXCEPT_KEY,
        ALL,
    }
}
