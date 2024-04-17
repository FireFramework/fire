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

package com.zto.fire.common.bean;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.zto.fire.common.util.JSONUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * MQ消费位点信息
 *
 * @author ChengLong
 * @version 2.4.5
 * @Date 2024/4/17 09:18
 */
public class ConsumerOffsetInfo {
    private String topic;
    private String groupId;
    private String broker;
    private Integer partition;
    private Long offset;
    private Long timestamp;

    public ConsumerOffsetInfo() {
    }

    public ConsumerOffsetInfo(String topic, String groupId, Integer partition, Long offset) {
        this.topic = topic;
        this.groupId = groupId;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = System.currentTimeMillis();
    }

    public ConsumerOffsetInfo(String topic, String groupId, String broker, Integer partition, Long offset, Long timestamp) {
        this.topic = topic;
        this.groupId = groupId;
        this.broker = broker;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
    }

    public ConsumerOffsetInfo(String topic, Integer partition, Long offset, Long timestamp) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getPartition() {
        return partition;
    }

    public void setPartition(Integer partition) {
        this.partition = partition;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getBroker() {
        return broker;
    }

    public void setBroker(String broker) {
        this.broker = broker;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    /**
     * 将JsonArray转为Set<TopicPartition>
     */
    public static Set<ConsumerOffsetInfo> jsonToBean(String jsonArray) {
        if (StringUtils.isBlank(jsonArray)) return Collections.EMPTY_SET;

        try {
            Set<ConsumerOffsetInfo> set = JSONUtils.newObjectMapperWithDefaultConf().readValue(jsonArray, new TypeReference<Set<ConsumerOffsetInfo>>(){});
            return set;
        } catch (JsonProcessingException e) {
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



    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConsumerOffsetInfo that = (ConsumerOffsetInfo) o;
        return Objects.equals(topic, that.topic) && Objects.equals(groupId, that.groupId) && Objects.equals(partition, that.partition) && Objects.equals(offset, that.offset) && Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, groupId, partition, offset, timestamp);
    }
}
