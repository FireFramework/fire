package com.zto.fire.flink.sql.connector.rocketmq;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.rocketmq.common.message.MessageExt;

import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * , SupportsReadingMetadata, SupportsWatermarkPushDown
 */
public class FireRocketMQDynamicSource implements ScanTableSource
        , SupportsReadingMetadata
        , SupportsWatermarkPushDown {
    private static final String VALUE_METADATA_PREFIX = "value.";

    /**
     * Data type that describes the final output of the source.
     */
    protected DataType producedDataType;

    private final DataType physicalDataType;

    private final DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat;

    private final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat;

    protected final int[] keyProjection;

    /**
     * Indices that determine the value fields and the target position in the produced row.
     */
    protected final int[] valueProjection;

    /**
     * Prefix that needs to be removed from fields when constructing the physical data type.
     */
    protected final @Nullable
    String keyPrefix;

    /**
     * Watermark strategy that is used to generate per-partition watermark.
     */
    protected @Nullable
    WatermarkStrategy<RowData> watermarkStrategy;

    private final Properties properties;

    private final boolean upsertMode;

    private List<String> metadataKeys;

    public FireRocketMQDynamicSource(
            DataType physicalDataType,
            DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat,
            DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat,
            int[] keyProjection,
            int[] valueProjection,
            String keyPrefix,
            Properties properties,
            boolean upsertMode) {
        this.physicalDataType = physicalDataType;
        this.producedDataType = physicalDataType;
        this.keyDecodingFormat = keyDecodingFormat;
        this.valueDecodingFormat = valueDecodingFormat;
        this.keyProjection = keyProjection;
        this.valueProjection = valueProjection;
        this.keyPrefix = keyPrefix;
        this.properties = properties;
        this.upsertMode = upsertMode;
        this.metadataKeys = Collections.emptyList();
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return valueDecodingFormat.getChangelogMode();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final DeserializationSchema<RowData> keyDeserialization =
                createDeserialization(runtimeProviderContext, keyDecodingFormat, keyProjection, keyPrefix);

        final DeserializationSchema<RowData> valueDeserialization =
                createDeserialization(runtimeProviderContext, valueDecodingFormat, valueProjection, null);

        final TypeInformation<RowData> typeInformation = runtimeProviderContext.createTypeInformation(physicalDataType);


        final FireRocketMQSource<RowData> sourceFunction = createRocketMQConsumer(keyDeserialization, valueDeserialization, typeInformation);

        return SourceFunctionProvider.of(sourceFunction, false);
    }

    @Override
    public DynamicTableSource copy() {
        FireRocketMQDynamicSource copy = new FireRocketMQDynamicSource(
                physicalDataType, keyDecodingFormat, valueDecodingFormat, keyProjection, valueProjection, keyPrefix, properties, upsertMode);
        copy.producedDataType = producedDataType;
        copy.metadataKeys = metadataKeys;
        copy.watermarkStrategy = watermarkStrategy;
        return copy;
    }

    @Override
    public String asSummaryString() {
        return "Rocket MQ Table Source";
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {

        final Map<String, DataType> metadataMap = new LinkedHashMap<>();

        // according to convention, the order of the final row must be
        // PHYSICAL + FORMAT METADATA + CONNECTOR METADATA
        // where the format metadata has highest precedence

        // add value format metadata with prefix
        valueDecodingFormat
                .listReadableMetadata()
                .forEach((key, value) -> metadataMap.put(VALUE_METADATA_PREFIX + key, value));

        // 添加 连接器的 metadata，此处为rocket mq
        Stream.of(ReadableMetadata.values())
                .forEachOrdered(m -> metadataMap.putIfAbsent(m.key, m.dataType));

        return metadataMap;
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        // 分开连接器和 json中自带的metadata

        // 找到json中可以提取的 metadata 的所有 key
        final List<String> formatMetadataKeys =
                metadataKeys.stream()
                        .filter(k -> k.startsWith(VALUE_METADATA_PREFIX))
                        .collect(Collectors.toList());

        // connector metadata key
        final List<String> connectorMetadataKeys = metadataKeys.stream()
                .filter(k -> !k.startsWith(VALUE_METADATA_PREFIX))
                .collect(Collectors.toList());

        // push down format metadata
        // 拿到所有json中可以获取的metadata
        final Map<String, DataType> formatMetadata = valueDecodingFormat.listReadableMetadata();
        if (formatMetadata.size() > 0) {
            // 去掉 value.前缀
            final List<String> requestedFormatMetadataKeys =
                    formatMetadataKeys.stream()
                            .map(k -> k.substring(VALUE_METADATA_PREFIX.length()))
                            .collect(Collectors.toList());
            valueDecodingFormat.applyReadableMetadata(requestedFormatMetadataKeys);
        }

        this.metadataKeys = connectorMetadataKeys;
        this.producedDataType = producedDataType;
    }

    @Override
    public void applyWatermark(WatermarkStrategy<RowData> watermarkStrategy) {
        this.watermarkStrategy = watermarkStrategy;
    }

    // 重写 equals
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final FireRocketMQDynamicSource that = (FireRocketMQDynamicSource) o;
        return Objects.equals(producedDataType, that.producedDataType)
                && Objects.equals(metadataKeys, that.metadataKeys)
                && Objects.equals(physicalDataType, that.physicalDataType)
                && Objects.equals(keyDecodingFormat, that.keyDecodingFormat)
                && Objects.equals(valueDecodingFormat, that.valueDecodingFormat)
                && Arrays.equals(keyProjection, that.keyProjection)
                && Arrays.equals(valueProjection, that.valueProjection)
                && Objects.equals(keyPrefix, that.keyPrefix)
                && Objects.equals(properties, that.properties)
                && Objects.equals(upsertMode, that.upsertMode)
                && Objects.equals(watermarkStrategy, that.watermarkStrategy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                producedDataType,
                metadataKeys,
                physicalDataType,
                keyDecodingFormat,
                valueDecodingFormat,
                keyProjection,
                valueProjection,
                keyPrefix,
                properties,
                upsertMode,
                watermarkStrategy);
    }

    protected FireRocketMQSource<RowData> createRocketMQConsumer(
            DeserializationSchema<RowData> keyDeserialization,
            DeserializationSchema<RowData> valueDeserialization,
            TypeInformation<RowData> producedTypeInfo) {

        final DynamicRocketMQDeserializationSchema.MetadataConverter[] metadataConverters =
                Stream.of(ReadableMetadata.values())
                        .filter(m -> metadataKeys.contains(m.key))
                        .map(m -> m.converter)
                        .toArray(DynamicRocketMQDeserializationSchema.MetadataConverter[]::new);

        // 是否含有 connector metadata
        final boolean hasMetadata = metadataKeys.size() > 0;

        // adjust physical arity with value format's metadata
        final int adjustedPhysicalArity =
                producedDataType.getChildren().size() - metadataKeys.size();

        // adjust value format projection to include value format's metadata columns at the end

        final int[] adjustedValueProjection =
                IntStream.concat(
                        IntStream.of(valueProjection),
                        IntStream.range(
                                keyProjection.length + valueProjection.length,
                                adjustedPhysicalArity))
                        .toArray();

        final RocketMQDeserializationSchema<RowData> rocketMQDeserializer =
                new DynamicRocketMQDeserializationSchema(
                        adjustedPhysicalArity,
                        keyDeserialization,
                        keyProjection,
                        valueDeserialization,
                        adjustedValueProjection,
                        hasMetadata,
                        metadataConverters,
                        producedTypeInfo,
                        upsertMode);

        return new FireRocketMQSource<>(rocketMQDeserializer, properties);
    }

    private @Nullable
    DeserializationSchema<RowData> createDeserialization(
            Context context,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> format,
            int[] projection,
            @Nullable String prefix) {
        if (format == null) {
            return null;
        }
        DataType physicalFormatDataType =
                DataTypeUtils.projectRow(this.physicalDataType, projection);
        if (prefix != null) {
            physicalFormatDataType = DataTypeUtils.stripRowPrefix(physicalFormatDataType, prefix);
        }
        return format.createRuntimeDecoder(context, physicalFormatDataType);
    }

    enum ReadableMetadata {
        TOPIC(
                "topic",
                DataTypes.STRING().notNull(),
                new DynamicRocketMQDeserializationSchema.MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(MessageExt msg) {

                        return StringData.fromString(msg.getTopic());
                    }
                }),
        TIMESTAMP(
                "timestamp",
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull(),
                new DynamicRocketMQDeserializationSchema.MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(MessageExt msg) {
                        return TimestampData.fromEpochMillis(msg.getBornTimestamp());
                    }
                }),

        OFFSET(
                "offset",
                DataTypes.BIGINT().notNull(),
                new DynamicRocketMQDeserializationSchema.MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(MessageExt msg) {
                        return msg.getQueueOffset();
                    }
                }),

        TAGS("tags",
                DataTypes.STRING().notNull(),
                new DynamicRocketMQDeserializationSchema.MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(MessageExt msg) {
                        return StringData.fromString(msg.getTags());
                    }
                }),
        BROKER(
                "broker",
                DataTypes.STRING().notNull(),
                new DynamicRocketMQDeserializationSchema.MetadataConverter() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Object read(MessageExt messageExt) {
                        return StringData.fromString(messageExt.getBrokerName());
                    }
                }
        ),
        QUEUE_ID(
                "queue_id",
                DataTypes.INT().notNull(),
                new DynamicRocketMQDeserializationSchema.MetadataConverter() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Object read(MessageExt messageExt) {
                        return messageExt.getQueueId();
                    }
                }
        )
        ;

        public final String key;

        public final DataType dataType;

        public final DynamicRocketMQDeserializationSchema.MetadataConverter converter;

        ReadableMetadata(String key, DataType dataType, DynamicRocketMQDeserializationSchema.MetadataConverter converter) {
            this.key = key;
            this.dataType = dataType;
            this.converter = converter;
        }
    }
}
