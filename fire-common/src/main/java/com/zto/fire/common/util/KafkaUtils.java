package com.zto.fire.common.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Kafka工具类
 *
 * @author ChengLong 2019-5-31 13:16:06
 */
public class KafkaUtils {
    // 当前kafka监控的groupId
    private static final String kafkaMonitor = "bigdata_kafka_monitor";
    private static final Logger logger = LoggerFactory.getLogger(KafkaUtils.class);
    private static final Map<String, String> kafkaCluster = new HashMap<>();

    static {
        // 大数据kafka集群
        kafkaCluster.put("bigdata", "192.168.25.80:9092,192.168.25.81:9092,192.168.25.82:9092,192.168.25.129:9092,192.168.25.130:9092,192.168.25.131:9092");
        // zms kafka集群
        kafkaCluster.put("zms", "192.168.11.101:9092,192.168.11.102:9092,192.168.11.103:9092,192.168.1.173:9092,192.168.5.29:9092,192.168.5.30:9092");
        // 新的kafka集群
        kafkaCluster.put("zmsNew", "192.168.73.31:9092,192.168.73.32:9092,192.168.73.33:9092,192.168.73.34:9092,192.168.73.35:9092,192.168.73.36:9092");
        // 测试环境集群
        kafkaCluster.put("test", "10.9.45.97:9092,10.9.15.38:9092,10.9.36.49:9092,10.9.36.50:9092");
        // 新增kafka集群(宋昉)
        kafkaCluster.put("kafka-ai", "10.9.30.13:9092,10.9.30.14:9092,10.9.30.15:9092,10.9.30.16:9092,10.9.30.17:9092");
    }

    /**
     * 根据kafka集群名称获取broker地址
     *
     * @param clusterName 集群名称
     * @return broker地址
     */
    public static String getBorkers(String clusterName) {
        if (StringUtils.isNotBlank(clusterName)) {
            return kafkaCluster.get(clusterName);
        }
        return kafkaCluster.get("zms");
    }

    /**
     * 获取指定topic每一个partition的最新offset
     *
     * @param host  broker地址
     * @param topic topic名称
     * @return partition offset
     */
    public static Map<TopicPartition, Long> getLogEndOffset(String host, String topic) {
        Map<TopicPartition, Long> endOffsets = new ConcurrentHashMap<>();
        KafkaConsumer<?, ?> consumer = createNewConsumer(host, kafkaMonitor);
        List<PartitionInfo> partitionInfoList = consumer.partitionsFor(topic);
        List<TopicPartition> topicPartitions = partitionInfoList.stream().map(pi -> new TopicPartition(topic, pi.partition())).collect(Collectors.toList());
        consumer.assign(topicPartitions);
        consumer.seekToEnd(topicPartitions);
        topicPartitions.forEach(topicPartition -> endOffsets.put(topicPartition, consumer.position(topicPartition)));
        consumer.close();
        return endOffsets;
    }

    /**
     * 创建新的kafka consumer
     *
     * @param host    kafka broker地址
     * @param groupId 对应的groupId
     * @return KafkaConsumer
     */
    public static KafkaConsumer<String, String> createNewConsumer(String host, String groupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, host);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put("auto.offset.reset", "earliest");
        return new KafkaConsumer<>(properties);
    }

    /**
     * 消费一条消息
     *
     * @param host  broker地址
     * @param topic topic信息
     * @return 一条消息记录
     */
    public static String getMsg(String host, String topic) {
        KafkaConsumer<String, String> kafkaConsumer = null;
        String msg = "";
        try {
            kafkaConsumer = createNewConsumer(host, kafkaMonitor);
            kafkaConsumer.subscribe(Arrays.asList(topic));
            ConsumerRecords<String, String> records = kafkaConsumer.poll(10000);
            for (ConsumerRecord<String, String> record : records) {
                msg = record.value();
                break;
            }
        } catch (Exception e) {
            logger.error("获取消息失败", e);
        } finally {
            if (kafkaConsumer != null) {
                kafkaConsumer.close();
            }
        }
        return msg;
    }

    /**
     * 获取大于指定时间戳的一条消息
     *
     * @param host  broker地址
     * @param topic topic信息
     * @param timestamp 消息时间戳
     * @return 一条消息记录
     */
    public static String getMsg(String host, String topic, Long timestamp) {
        KafkaConsumer<String, String> kafkaConsumer = null;
        String msg = "";
        try {
            kafkaConsumer = createNewConsumer(host, kafkaMonitor);

            // 如果指定了时间戳，则取大于该时间戳的消息
            if (timestamp != null) {
                // 获取topic的partition信息
                List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(topic);
                List<TopicPartition> topicPartitions = new ArrayList<>();
                Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();

                for(PartitionInfo partitionInfo : partitionInfos) {
                    topicPartitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
                    timestampsToSearch.put(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()), timestamp);
                }
                // 手动指定各分区offset
                kafkaConsumer.assign(topicPartitions);

                // 获取每个partition指定时间戳的偏移量
                Map<TopicPartition, OffsetAndTimestamp> map = kafkaConsumer.offsetsForTimes(timestampsToSearch);
                System.out.println("根据时间戳获取偏移量：map.size=" + map.size());
                OffsetAndTimestamp offsetTimestamp = null;
                System.out.println("开始设置各分区初始偏移量...");
                for(Map.Entry<TopicPartition, OffsetAndTimestamp> entry : map.entrySet()) {
                    // 如果设置的查询偏移量的时间点大于最大的索引记录时间，那么value就为空
                    offsetTimestamp = entry.getValue();
                    if(offsetTimestamp != null) {
                        // 设置读取消息的偏移量
                        Long offset = offsetTimestamp.offset();
                        kafkaConsumer.seek(entry.getKey(), offset);
                        System.out.println("seek: id=" + entry.getKey().partition() + " offset=" + offset);
                    }
                }
            } else {
                // 如果未指定时间戳，则直接获取消息
                kafkaConsumer.subscribe(Arrays.asList(topic));
            }

            // 消费消息
            ConsumerRecords<String, String> records = kafkaConsumer.poll(10000);
            for (ConsumerRecord<String, String> record : records) {
                if (timestamp == null) {
                    msg = record.value();
                    break;
                } else {
                    // 如果指定时间戳，则取大于指定时间戳的消息
                    if (record.timestamp() >= timestamp) {
                        msg = record.value();
                        break;
                    }
                }
            }
        } catch (Exception e) {
            logger.error("获取消息失败", e);
        } finally {
            if (kafkaConsumer != null) {
                kafkaConsumer.close();
            }
        }
        return msg;
    }
}
