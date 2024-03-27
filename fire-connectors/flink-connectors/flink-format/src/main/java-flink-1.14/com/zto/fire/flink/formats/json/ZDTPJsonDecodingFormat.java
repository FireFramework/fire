/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zto.fire.flink.formats.json;

import com.zto.fire.flink.formats.json.ZDTPJsonDeserializationSchema.MetadataConverter;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class ZDTPJsonDecodingFormat implements DecodingFormat<DeserializationSchema<RowData>> {
    private List<String> metadataKeys;
    private final boolean ignoreParseErrors;

    private final boolean cdcWriteHive;

    private final TimestampFormat timestampFormatOption;

    public ZDTPJsonDecodingFormat(boolean ignoreParseErrors, boolean cdcWriteHive, TimestampFormat timestampFormatOption) {
        this.ignoreParseErrors = ignoreParseErrors;
        this.cdcWriteHive = cdcWriteHive;
        this.timestampFormatOption = timestampFormatOption;
        this.metadataKeys = Collections.emptyList();
    }

    @Override
    public DeserializationSchema<RowData> createRuntimeDecoder(DynamicTableSource.Context context, DataType physicalDataType) {

        final List<ReadableMetadata> readableMetadata =
                Stream.of(ReadableMetadata.values())
                        .filter(m -> metadataKeys.contains(m.key))
                        .collect(Collectors.toList());


        final List<DataTypes.Field> metadataFields =
                readableMetadata.stream()
                        .map(m -> DataTypes.FIELD(m.key, m.dataType))
                        .collect(Collectors.toList());

        final DataType producedDataType = DataTypeUtils.appendRowFields(physicalDataType, metadataFields);

        final TypeInformation<RowData> producedTypeInfo =
                context.createTypeInformation(producedDataType);


        return new ZDTPJsonDeserializationSchema(
                physicalDataType, readableMetadata, producedTypeInfo, ignoreParseErrors, timestampFormatOption);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return cdcWriteHive ? ChangelogMode.insertOnly() : ChangelogMode.all();
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        final Map<String, DataType> metadataMap = new LinkedHashMap<>();
        Stream.of(ReadableMetadata.values())
                .forEachOrdered(m -> metadataMap.put(m.key, m.dataType));
        return metadataMap;
    }

    // 所有 json中的metadataKeys
    @Override
    public void applyReadableMetadata(List<String> metadataKeys) {
        this.metadataKeys = metadataKeys;
    }

    /**
     * zdtp 字段中的metadata
     * {
     * "schema":"route_order",
     * "table":"route_order_001",
     * "gtid":"ldjfdlfj-ldfjdl",
     * "logFile":"0002334.bin",
     * "offset":"dfle-efe",
     * "pos":12384384,
     * "when":123943434383,
     * "before":{},
     * "after":{}
     * }
     */
    enum ReadableMetadata {
        SCHEMA(
                "schema",
                DataTypes.STRING().nullable(),
                DataTypes.FIELD("schema", DataTypes.STRING()),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(GenericRowData row, int pos) {
                        return row.getString(pos);
                    }
                }),
        TABLE(
                "table",
                DataTypes.STRING().nullable(),
                DataTypes.FIELD("table", DataTypes.STRING()),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(GenericRowData row, int pos) {
                        return row.getString(pos);
                    }
                }),
        GTID(
                "gtid",
                DataTypes.STRING().nullable(),
                DataTypes.FIELD("gtid", DataTypes.STRING()),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(GenericRowData row, int pos) {
                        return row.getString(pos);
                    }
                }),
        LOGFILE(
                "logFile",
                DataTypes.STRING().nullable(),
                DataTypes.FIELD("logFile", DataTypes.STRING()),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(GenericRowData row, int pos) {
                        return row.getString(pos);
                    }
                }),
        OFFSET(
                "offset",
                DataTypes.STRING().nullable(),
                DataTypes.FIELD("offset", DataTypes.STRING()),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(GenericRowData row, int pos) {
                        return row.getString(pos);
                    }
                }),
        POS(
                "pos",
                DataTypes.BIGINT().nullable(),
                DataTypes.FIELD("pos", DataTypes.BIGINT()),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(GenericRowData row, int pos) {
                        return row.getLong(pos);
                    }
                }),
        WHEN(
                "when",
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).nullable(),
                DataTypes.FIELD("when", DataTypes.BIGINT()),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(GenericRowData row, int pos) {
                        if (row.isNullAt(pos)) return null;
                        return TimestampData.fromEpochMillis(row.getLong(pos));
                    }
                }),
        // 将binlog中的RowKind带出去，当作元数据
        ROW_KIND(
                "row_kind",
                DataTypes.STRING().notNull(),
                DataTypes.FIELD("row_kind", DataTypes.STRING()),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Object convert(GenericRowData row, int pos) {

                        return StringData.fromString(row.getRowKind().toString());
                    }
                }
        )
        ;

        final String key;

        final DataType dataType;

        final DataTypes.Field requiredJsonField;

        final MetadataConverter converter;

        ReadableMetadata(String key, DataType dataType, DataTypes.Field requiredJsonField, MetadataConverter converter) {
            this.key = key;
            this.dataType = dataType;
            this.requiredJsonField = requiredJsonField;
            this.converter = converter;
        }
    }
}
