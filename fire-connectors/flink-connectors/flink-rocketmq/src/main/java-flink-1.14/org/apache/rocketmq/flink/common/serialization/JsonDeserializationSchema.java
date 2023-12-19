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

package org.apache.rocketmq.flink.common.serialization;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.rocketmq.common.message.MessageExt;

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
    public RowData deserializeTagKeyAndValue(MessageExt msg) {
        byte[] value = msg.getBody();
        String valueString = value != null ? new String(value, StandardCharsets.UTF_8) : null;

        if (value != null) {
            try {
                JSONObject json = JSONObject.parseObject(valueString);
                json.put("topic", msg.getTopic());
                json.put("partition", msg.getQueueId());
                json.put("offset", msg.getQueueOffset());
                json.put("timestamp", msg.getBornTimestamp());
                json.put("key", msg.getKeys());
                json.put("tag", msg.getTags());
                byte[] bytes = json.toJSONString().getBytes(StandardCharsets.UTF_8);
                // 调用sql connector的format进行反序列化
                return this.value.deserialize(bytes);
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
