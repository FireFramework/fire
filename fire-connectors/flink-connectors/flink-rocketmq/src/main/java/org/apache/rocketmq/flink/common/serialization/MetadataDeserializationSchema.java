/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.flink.common.serialization;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * 反序列化MessageExt，将消息体中包含的key、tag、topic、timestamp等元数据信息返回
 *
 * @author ChengLong 2021-5-10 09:44:55
 */
public class MetadataDeserializationSchema implements TagKeyValueDeserializationSchema<MessageExt> {

    @Override
    public MessageExt deserializeTagKeyAndValue(MessageExt msg) {
        return msg;
    }

    @Override
    public TypeInformation<MessageExt> getProducedType() {
        return TypeInformation.of(new TypeHint<MessageExt>(){});
    }

}
