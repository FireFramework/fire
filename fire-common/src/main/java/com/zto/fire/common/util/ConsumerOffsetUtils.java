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

package com.zto.fire.common.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.zto.fire.common.bean.ConsumerOffsetInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * MQ消费位点工具类
 *
 * @author ChengLong
 * @version 2.3.
 * @Date 2024/4/18 11:09
 */
public class ConsumerOffsetUtils {

    /**
     * 将JsonArray转为Set<TopicPartition>
     */
    public static Set<ConsumerOffsetInfo> jsonToBean(String jsonArray) {
        if (StringUtils.isBlank(jsonArray)) return Collections.EMPTY_SET;

        try {
            return JSONUtils.newObjectMapperWithDefaultConf().readValue(jsonArray, new TypeReference<Set<ConsumerOffsetInfo>>(){});
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 将消费位点信息转为Kafka TopicPartition
     */
    public static Map<TopicPartition, Long> toKafkaTopicPartition(Set<ConsumerOffsetInfo> topicPartitionInfoSet) {
        Map<TopicPartition, Long> topicPartitionLongMap = new LinkedHashMap<>();
        if (topicPartitionInfoSet == null || topicPartitionInfoSet.isEmpty()) return topicPartitionLongMap;

        topicPartitionInfoSet.forEach(info -> {
            topicPartitionLongMap.put(new TopicPartition(info.getTopic(), info.getPartition()), info.getOffset());
        });

        return topicPartitionLongMap;
    }

    /**
     * 将消费位点信息转为Kafka TopicPartition
     */
    public static Map<TopicPartition, Long> toKafkaTopicPartition(String jsonArray) {
        return toKafkaTopicPartition(jsonToBean(jsonArray));
    }

    /**
     * 将消费位点信息转为RocketMQ MessageQueue
     */
    public static Map<MessageQueue, Long> toRocketMQTopicPartition(Set<ConsumerOffsetInfo> topicPartitionInfoSet) {
        Map<MessageQueue, Long> topicPartitionLongMap = new LinkedHashMap<>();
        if (topicPartitionInfoSet == null || topicPartitionInfoSet.isEmpty()) return topicPartitionLongMap;

        topicPartitionInfoSet.forEach(info -> {
            topicPartitionLongMap.put(new MessageQueue(info.getTopic(), info.getBroker(), info.getPartition()), info.getOffset());
        });

        return topicPartitionLongMap;
    }

    /**
     * 将消费位点信息转为RocketMQ MessageQueue
     */
    public static Map<MessageQueue, Long> toRocketMQTopicPartition(String jsonArray) {
        return toRocketMQTopicPartition(jsonToBean(jsonArray));
    }
}
