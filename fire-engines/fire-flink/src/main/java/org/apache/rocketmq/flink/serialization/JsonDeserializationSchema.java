package org.apache.rocketmq.flink.serialization;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * 将rocketmq消息反序列化成RowData
 * @author ChengLong 2021-5-9 13:40:17
 */
public class JsonDeserializationSchema implements TagKeyValueDeserializationSchema<RowData> {
    private DeserializationSchema<RowData> key;
    private DeserializationSchema<RowData> value;

    public JsonDeserializationSchema(DeserializationSchema<RowData> key, DeserializationSchema<RowData> value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public RowData deserializeTagKeyAndValue(byte[] tag, byte[] key, byte[] value) {
        String keyString = key != null ? new String(key, StandardCharsets.UTF_8) : null;
        String valueString = value != null ? new String(value, StandardCharsets.UTF_8) : null;
        if (value != null) {
            try {
                // 调用sql connector的format进行反序列化
                return this.value.deserialize(value);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return TypeInformation.of(new TypeHint<RowData>(){});
    }
}
