//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitMode;
import org.apache.flink.streaming.connectors.kafka.internals.*;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.PropertiesUtil;
import org.apache.flink.util.SerializedValue;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

/** @deprecated */
@Deprecated
@PublicEvolving
public class FlinkKafkaConsumer<T> extends FlinkKafkaConsumerBase<T> {
    private static final long serialVersionUID = 1L;
    public static final String KEY_POLL_TIMEOUT = "flink.poll-timeout";
    public static final long DEFAULT_POLL_TIMEOUT = 100L;
    protected final Properties properties;
    protected final long pollTimeout;

    public FlinkKafkaConsumer(String topic, DeserializationSchema<T> valueDeserializer, Properties props) {
        this(Collections.singletonList(topic), valueDeserializer, props);
    }

    public FlinkKafkaConsumer(String topic, KafkaDeserializationSchema<T> deserializer, Properties props) {
        this(Collections.singletonList(topic), deserializer, props);
    }

    public FlinkKafkaConsumer(List<String> topics, DeserializationSchema<T> deserializer, Properties props) {
        this((List)topics, (KafkaDeserializationSchema)(new KafkaDeserializationSchemaWrapper(deserializer)), props);
    }

    public FlinkKafkaConsumer(List<String> topics, KafkaDeserializationSchema<T> deserializer, Properties props) {
        this(topics, (Pattern)null, deserializer, props);
    }

    public FlinkKafkaConsumer(Pattern subscriptionPattern, DeserializationSchema<T> valueDeserializer, Properties props) {
        this((List)null, subscriptionPattern, new KafkaDeserializationSchemaWrapper(valueDeserializer), props);
    }

    public FlinkKafkaConsumer(Pattern subscriptionPattern, KafkaDeserializationSchema<T> deserializer, Properties props) {
        this((List)null, subscriptionPattern, deserializer, props);
    }

    private FlinkKafkaConsumer(List<String> topics, Pattern subscriptionPattern, KafkaDeserializationSchema<T> deserializer, Properties props) {
        super(topics, subscriptionPattern, deserializer, PropertiesUtil.getLong((Properties)Preconditions.checkNotNull(props, "props"), "flink.partition-discovery.interval-millis", Long.MIN_VALUE), !PropertiesUtil.getBoolean(props, "flink.disable-metrics", false));
        this.properties = props;
        setDeserializer(this.properties);

        try {
            if (this.properties.containsKey("flink.poll-timeout")) {
                this.pollTimeout = Long.parseLong(this.properties.getProperty("flink.poll-timeout"));
            } else {
                this.pollTimeout = 100L;
            }

        } catch (Exception var6) {
            throw new IllegalArgumentException("Cannot parse poll timeout for 'flink.poll-timeout'", var6);
        }
    }

    protected AbstractFetcher<T, ?> createFetcher(SourceFunction.SourceContext<T> sourceContext, Map<KafkaTopicPartition, Long> assignedPartitionsWithInitialOffsets, SerializedValue<WatermarkStrategy<T>> watermarkStrategy, StreamingRuntimeContext runtimeContext, OffsetCommitMode offsetCommitMode, MetricGroup consumerMetricGroup, boolean useMetrics) throws Exception {
        adjustAutoCommitConfig(this.properties, offsetCommitMode);
        return new KafkaFetcher(sourceContext, assignedPartitionsWithInitialOffsets, watermarkStrategy, runtimeContext.getProcessingTimeService(), runtimeContext.getExecutionConfig().getAutoWatermarkInterval(), runtimeContext.getUserCodeClassLoader(), runtimeContext.getTaskNameWithSubtasks(), this.deserializer, this.properties, this.pollTimeout, runtimeContext.getMetricGroup(), consumerMetricGroup, useMetrics);
    }

    protected AbstractPartitionDiscoverer createPartitionDiscoverer(KafkaTopicsDescriptor topicsDescriptor, int indexOfThisSubtask, int numParallelSubtasks) {
        return new KafkaPartitionDiscoverer(topicsDescriptor, indexOfThisSubtask, numParallelSubtasks, this.properties);
    }

    protected Map<KafkaTopicPartition, Long> fetchOffsetsWithTimestamp(Collection<KafkaTopicPartition> partitions, long timestamp) {
        Map<TopicPartition, Long> partitionOffsetsRequest = new HashMap(partitions.size());
        Iterator var5 = partitions.iterator();

        while(var5.hasNext()) {
            KafkaTopicPartition partition = (KafkaTopicPartition)var5.next();
            partitionOffsetsRequest.put(new TopicPartition(partition.getTopic(), partition.getPartition()), timestamp);
        }

        Map<KafkaTopicPartition, Long> result = new HashMap(partitions.size());
        KafkaConsumer<?, ?> consumer = new KafkaConsumer(this.properties);
        Throwable var7 = null;

        try {
            Iterator var8 = consumer.offsetsForTimes(partitionOffsetsRequest).entrySet().iterator();

            while(var8.hasNext()) {
                Map.Entry<TopicPartition, OffsetAndTimestamp> partitionToOffset = (Map.Entry)var8.next();
                result.put(new KafkaTopicPartition(((TopicPartition)partitionToOffset.getKey()).topic(), ((TopicPartition)partitionToOffset.getKey()).partition()), partitionToOffset.getValue() == null ? null : ((OffsetAndTimestamp)partitionToOffset.getValue()).offset());
            }
        } catch (Throwable var17) {
            var7 = var17;
            throw var17;
        } finally {
            if (consumer != null) {
                if (var7 != null) {
                    try {
                        consumer.close();
                    } catch (Throwable var16) {
                        var7.addSuppressed(var16);
                    }
                } else {
                    consumer.close();
                }
            }

        }

        return result;
    }

    protected boolean getIsAutoCommitEnabled() {
        return PropertiesUtil.getBoolean(this.properties, "enable.auto.commit", true) && PropertiesUtil.getLong(this.properties, "auto.commit.interval.ms", 5000L) > 0L;
    }

    private static void setDeserializer(Properties props) {
        String deSerName = ByteArrayDeserializer.class.getName();
        Object keyDeSer = props.get("key.deserializer");
        Object valDeSer = props.get("value.deserializer");
        if (keyDeSer != null && !keyDeSer.equals(deSerName)) {
            LOG.warn("Ignoring configured key DeSerializer ({})", "key.deserializer");
        }

        if (valDeSer != null && !valDeSer.equals(deSerName)) {
            LOG.warn("Ignoring configured value DeSerializer ({})", "value.deserializer");
        }

        props.put("key.deserializer", deSerName);
        props.put("value.deserializer", deSerName);
    }

    // TODO: ------------ start：二次开发代码 ----------------- //
    @Override
    public void open(Configuration configuration) throws Exception {
        try {
            String topics = this.properties.getProperty("kafka.topics", "");
            String groupId = this.properties.getProperty("group.id", "");

            // 是否开启周期性的offset提交，仅在开启checkpoint的情况下生效
            this.enableForceAutoCommit = Boolean.parseBoolean(this.properties.getProperty("kafka.force.autoCommit.enable", "false"));
            // 自动提交offset的周期
            this.forceAutoCommitIntervalMillis = Long.parseLong(this.properties.getProperty("kafka.force.autoCommit.Interval", "30000"));
            if (this.enableForceAutoCommit) {
                this.executeAutoCommit(topics, groupId);
                LOG.info("开启异步提交kafka offset功能，topics：{} groupId：{} interval：{}", topics, groupId, this.forceAutoCommitIntervalMillis);
            }

            // 判断是否跳过从状态中读取到的offset信息
            boolean skipRestoredState = Boolean.parseBoolean(this.properties.getProperty("kafka.force.overwrite.stateOffset.enable", "false"));

            if (skipRestoredState && this.restoredState != null) {
                // 将状态中的offseet置为null以后，将在super.open方法中取消从状态seek offset
                this.restoredState = null;
                LOG.info("将忽略状态中的offset信息，使用默认策略消费kafka消息！topics：{} groupId：{}", topics, groupId);
            }
        } catch (Exception e) {
            LOG.error("强制覆盖状态中的offset或周期性提交offset功能开启失败", e);
        } finally {
            super.open(configuration);
        }
    }

    /**
     * 周期性提交kafka offset.
     * @param topics
     * kafka的topic列表
     * @param groupId
     * 为该groupId执行异步的offset提交
     */
    private void executeAutoCommit(String topics, String groupId) {
        ExecutorService service = Executors.newSingleThreadExecutor();
        service.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    long threadId = Thread.currentThread().getId();
                    while (true) {
                        if (running && kafkaFetcher != null && (offsetCommitMode == OffsetCommitMode.ON_CHECKPOINTS)) {
                            HashMap<KafkaTopicPartition, Long> currentOffsets = kafkaFetcher.snapshotCurrentState();
                            kafkaFetcher.commitInternalOffsetsToKafka(currentOffsets, offsetCommitCallback);
                            LOG.warn("周期性自动提交kafka offset成功！topics：{} groupId：{} ThreadId：{}", topics, groupId, threadId);
                        }
                        Thread.sleep(forceAutoCommitIntervalMillis);
                    }
                } catch (Exception e) {
                    LOG.error("周期性自动提交offset定时任务执行出错", e);
                }
            }
        });
    }
    // TODO: ------------ end：二次开发代码 ----------------- //
}
