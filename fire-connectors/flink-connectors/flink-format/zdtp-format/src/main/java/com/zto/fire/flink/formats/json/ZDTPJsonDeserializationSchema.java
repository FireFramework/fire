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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.lang.String.format;


public class ZDTPJsonDeserializationSchema implements DeserializationSchema<RowData> {
    private static final long serialVersionUID = 1L;
    private static final String OP_TYPE = "op_type";
    private static final String BEFORE = "before";
    private static final String AFTER = "after";
    private static final String OP_INSERT = "I";
    private static final String OP_UPDATE = "U";
    private static final String OP_DELETE = "D";

    /** Flag that indicates that an additional projection is required for metadata. */
    private final boolean hasMetadata;

    private final JsonRowDataDeserializationSchema jsonDeserializer;

    /** Metadata to be extracted for every record. */
    private final MetadataConverter[] metadataConverters;

    private final TypeInformation<RowData> producedTypeInfo;

    private final List<String> fieldNames;

    private final boolean ignoreParseErrors;


    private final int fieldCount;

    public ZDTPJsonDeserializationSchema(
            DataType physicalDataType,
            List<ZDTPJsonDecodingFormat.ReadableMetadata> readableMetadata,
            TypeInformation<RowData> producedTypeInfo,
            boolean ignoreParseErrors,
            TimestampFormat timestampFormatOption) {
        final RowType jsonRowType = createJsonRowType(physicalDataType, readableMetadata);
        this.jsonDeserializer =
                new JsonRowDataDeserializationSchema(
                        jsonRowType,
                        producedTypeInfo,
                        false,
                        ignoreParseErrors,
                        timestampFormatOption);
        this.hasMetadata = readableMetadata.size() > 0;

        this.metadataConverters = createMetadataConverters(jsonRowType, readableMetadata);

        this.producedTypeInfo = producedTypeInfo;

        this.ignoreParseErrors = ignoreParseErrors;

        final RowType physicalRowType = (RowType) physicalDataType.getLogicalType();

        this.fieldNames = physicalRowType.getFieldNames();

        this.fieldCount = physicalRowType.getFieldCount();
    }
    @Override
    public RowData deserialize(byte[] message) throws IOException {
        throw new RuntimeException(
                "Please invoke DeserializationSchema#deserialize(byte[], Collector<RowData>) instead.");
    }

    @Override
    public void deserialize(byte[] message, Collector<RowData> collector) throws IOException {
        try {
            GenericRowData row = (GenericRowData) jsonDeserializer.deserialize(message);
            String type = row.getString(0).toString();
            GenericRowData before = (GenericRowData) row.getField(1); // before filed
            GenericRowData after = (GenericRowData) row.getField(2); // after field
            if (OP_INSERT.equals(type)) {
                // "data" field is a row, contains inserted rows
                after.setRowKind(RowKind.INSERT);
                emitRow(row, after, collector);
            } else if (OP_UPDATE.equals(type)) {
                before.setRowKind(RowKind.UPDATE_BEFORE);
                after.setRowKind(RowKind.UPDATE_AFTER);
                emitRow(row, before, collector);
                emitRow(row, after, collector);
            } else if (OP_DELETE.equals(type)) {
                before.setRowKind(RowKind.DELETE);
                emitRow(row, before, collector);
            } else {
                if (!ignoreParseErrors) {
                    throw new IOException(
                            format(
                                    "Unknown \"type\" value \"%s\". The Maxwell JSON message is '%s'",
                                    type, new String(message)));
                }
            }
        }catch (Throwable t) {
            if (!ignoreParseErrors) {
                throw new IOException(
                        format("Corrupt ZDTP JSON message '%s'.", new String(message)), t);
            }
        }
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;

        if (obj == null || getClass() != obj.getClass()) return false;

        ZDTPJsonDeserializationSchema that = (ZDTPJsonDeserializationSchema) obj;
        return ignoreParseErrors == that.ignoreParseErrors
                && fieldCount == that.fieldCount
                && Objects.equals(jsonDeserializer, that.jsonDeserializer)
                && Objects.equals(producedTypeInfo, that.producedTypeInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                jsonDeserializer,
                producedTypeInfo,
                ignoreParseErrors,
                fieldCount);
    }

    private void emitRow(GenericRowData rootRow, GenericRowData physicalRow, Collector<RowData> collector) {
        if (!hasMetadata) {
            collector.collect(physicalRow);
            return;
        }
        int physicalArity = physicalRow.getArity();
        int metadataArity = metadataConverters.length;

        GenericRowData producedRow = new GenericRowData(physicalRow.getRowKind(), physicalArity + metadataArity);

        for (int physicalPos = 0; physicalPos < physicalArity; physicalPos ++) {
            producedRow.setField(physicalPos, physicalRow.getField(physicalPos));
        }
        RowKind rootRowRowKind = rootRow.getRowKind();
        // `row_kind` STRING METADATA FROM 'value.row_kind' VIRTUAL 从rootRow convert 元数据时，
        // 由于rootRow的RowKind总是为 INSERT, 获取不到预期的结果，所以每次设置为physicalRow的 RowKind
        rootRow.setRowKind(physicalRow.getRowKind());
        for (int metadataPos = 0; metadataPos < metadataArity; metadataPos ++) {
            producedRow.setField(physicalArity + metadataPos, metadataConverters[metadataPos].convert(rootRow));
        }
        // 还原rootRow的RowKind
        rootRow.setRowKind(rootRowRowKind);
        collector.collect(producedRow);
    }

    private static RowType createJsonRowType(
            DataType physicalDataType, List<ZDTPJsonDecodingFormat.ReadableMetadata> readableMetadata) {

        DataType root =
                DataTypes.ROW(
                        DataTypes.FIELD(OP_TYPE, DataTypes.STRING()),
                        DataTypes.FIELD(BEFORE, physicalDataType),
                        DataTypes.FIELD(AFTER, physicalDataType));
        // append fields that are required for reading metadata in the root
        final List<DataTypes.Field> rootMetadataFields =
                readableMetadata.stream()
                        .map(m -> m.requiredJsonField)
                        .distinct()
                        .collect(Collectors.toList());
        return (RowType) DataTypeUtils.appendRowFields(root, rootMetadataFields).getLogicalType();
    }

    private static MetadataConverter[] createMetadataConverters(
            RowType jsonRowType, List<ZDTPJsonDecodingFormat.ReadableMetadata> requestedMetadata) {
        return requestedMetadata.stream()
                .map(m -> convert(jsonRowType, m))
//                .map(m -> (MetadataConverter) (row, pos) -> m.converter.convert(row, jsonRowType.getFieldNames().indexOf(m.requiredJsonField.getName())))
                .toArray(MetadataConverter[]::new);
    }

    private static MetadataConverter convert(RowType jsonRowType, ZDTPJsonDecodingFormat.ReadableMetadata metadata) {
        final int pos = jsonRowType.getFieldNames().indexOf(metadata.requiredJsonField.getName());
        return (root, unused) -> metadata.converter.convert(root, pos);
    }

    interface MetadataConverter extends Serializable {
        default Object convert(GenericRowData row) {
            return convert(row, -1);
        }

        Object convert(GenericRowData row, int pos);
    }
}
