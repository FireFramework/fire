package com.zto.fire.flink.sql.connector.rocketmq;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.DeserializationException;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.rocketmq.common.message.MessageExt;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class DynamicRocketMQDeserializationSchema implements RocketMQDeserializationSchema<RowData> {

    private final @Nullable DeserializationSchema<RowData> keyDeserialization;

    private final DeserializationSchema<RowData> valueDeserialization;

    private final boolean hasMetadata;

    private final KeyBufferCollector keyBufferCollector;

    private final OutputProjectionCollector outputCollector;

    private final TypeInformation<RowData> producedTypeInfo;

    private final boolean upsertMode;

    public DynamicRocketMQDeserializationSchema(
            int physicalArity,
            DeserializationSchema<RowData> keyDeserialization,
            int[] keyProjection,
            DeserializationSchema<RowData> valueDeserialization,
            int[] valueProjection,
            boolean hasMetadata,
            MetadataConverter[] metadataConverters,
            TypeInformation<RowData> producedTypeInfo,
            boolean upsertMode) {
        this.keyDeserialization = keyDeserialization;
        this.valueDeserialization = valueDeserialization;
        this.hasMetadata = hasMetadata;
        this.producedTypeInfo = producedTypeInfo;
        this.keyBufferCollector = new KeyBufferCollector();
        this.outputCollector = new OutputProjectionCollector(
                physicalArity, keyProjection, valueProjection, metadataConverters, upsertMode);

        this.upsertMode = upsertMode;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        if (keyDeserialization != null) keyDeserialization.open(context);

        valueDeserialization.open(context);
    }

    @Override
    public RowData deserialize(MessageExt message) throws Exception {
        throw new IllegalStateException("A collector is required for deserializing.");
    }

    @Override
    public void deserialize(MessageExt message, Collector<RowData> collector) throws IOException {
        // shortcut in case no output projection is required,
        // also not for a cartesian product with the keys
        if (keyDeserialization == null && !hasMetadata) {
            valueDeserialization.deserialize(message.getBody(), collector);
            return;
        }
        // buffer key(s)
        if (keyDeserialization != null) {
            keyDeserialization.deserialize(message.getKeys().getBytes(), keyBufferCollector);
        }

        // project output while emitting values
        outputCollector.inputRecord = message;
        outputCollector.physicalKeyRows = keyBufferCollector.buffer;
        outputCollector.outputCollector = collector;

        if (message.getBody() == null && upsertMode) {
            // collect tombstone messages in upsert mode by hand
            outputCollector.collect(null);
        } else {
            valueDeserialization.deserialize(message.getBody(), outputCollector);
        }
        keyBufferCollector.buffer.clear();
    }

    @Override
    public boolean isEndOfStream(RowData record) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }

    interface MetadataConverter extends Serializable {
        Object read(MessageExt messageExt);
    }

    private static final class KeyBufferCollector implements Collector<RowData>, Serializable {
        private static final long serialVersionUID = 1L;
        private final List<RowData> buffer = new ArrayList<>();
        @Override
        public void collect(RowData record) {
            buffer.add(record);
        }

        @Override
        public void close() {
            // do nothing
        }
    }

    private static final class OutputProjectionCollector
            implements Collector<RowData>, Serializable {

        private static final long serialVersionUID = 1L;

        private final int physicalArity;

        private final int[] keyProjection;

        private final int[] valueProjection;

        private final MetadataConverter[] metadataConverters;

        private final boolean upsertMode;

        private transient MessageExt inputRecord;

        private transient List<RowData> physicalKeyRows;

        private transient Collector<RowData> outputCollector;

        OutputProjectionCollector(
                int physicalArity,
                int[] keyProjection,
                int[] valueProjection,
                MetadataConverter[] metadataConverters,
                boolean upsertMode) {
            this.physicalArity = physicalArity;
            this.keyProjection = keyProjection;
            this.valueProjection = valueProjection;
            this.metadataConverters = metadataConverters;
            this.upsertMode = upsertMode;
        }

        @Override
        public void collect(RowData physicalValueRow) {
            // no key defined
            if (keyProjection.length == 0) {
                emitRow(null, (GenericRowData) physicalValueRow);
                return;
            }

            // otherwise emit a value for each key
            for (RowData physicalKeyRow : physicalKeyRows) {
                emitRow((GenericRowData) physicalKeyRow, (GenericRowData) physicalValueRow);
            }
        }

        @Override
        public void close() {
            // nothing to do
        }

        private void emitRow(
                @Nullable GenericRowData physicalKeyRow,
                @Nullable GenericRowData physicalValueRow) {
            final RowKind rowKind;
            if (physicalValueRow == null) {
                if (upsertMode) {
                    rowKind = RowKind.DELETE;
                } else {
                    throw new DeserializationException(
                            "Invalid null value received in non-upsert mode. Could not to set row kind for output record.");
                }
            } else {
                rowKind = physicalValueRow.getRowKind();
            }

            final int metadataArity = metadataConverters.length;
            // key中的值和 before after 中的值位置不变， value中的metadata 放后面，connector中的metadata放最后
            /**
             * create table orders(
             * `tb` STRING metadata FROM 'value.table',
             * id string,
             * `db` STRING METADATA FROM 'value.schema',
             * name STRING,
             * k_age int,
             * description string,
             * `event_time` TIMESTAMP(3) METADATA FROM 'timestamp',
             * k_kk string,
             * weight float
             * ) WITH (
             * 'connector' = 'rocket-mq',
             * 'topic' = 'test0',
             * 'properties.bootstrap.servers' = 'localhost:9876',
             * 'properties.group.id' = 'test_read',
             * 'scan.startup.mode' = 'latest-offset',
             * 'format' = 'maxwell-json',
             * 'key.format' = 'json',
             * 'key.fields-prefix' = 'k_',
             * 'key.fields' = 'k_age;k_kk',
             * 'value.fields-include' = 'EXCEPT_KEY'
             * )
             *
             * 应该位置如下设置值
             *
             *  0   1    2        3           4      5     6   7    8
             * id, name, k_age, description, k_kk, weight, tb, db, event_time
             */

            final GenericRowData producedRow =
                    new GenericRowData(rowKind, physicalArity + metadataArity);

            for (int keyPos = 0; keyPos < keyProjection.length; keyPos++) {
                assert physicalKeyRow != null;
                producedRow.setField(keyProjection[keyPos], physicalKeyRow.getField(keyPos));
            }

            if (physicalValueRow != null) {
                for (int valuePos = 0; valuePos < valueProjection.length; valuePos++) {
                    producedRow.setField(
                            valueProjection[valuePos], physicalValueRow.getField(valuePos));
                }
            }

            // 为什么connector 的metadata在最后一位？
            for (int metadataPos = 0; metadataPos < metadataArity; metadataPos++) {
                producedRow.setField(
                        physicalArity + metadataPos,
                        metadataConverters[metadataPos].read(inputRecord));
            }
            outputCollector.collect(producedRow);
        }
    }
}
