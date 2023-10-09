/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.rocketmq.flink;

import avro.shaded.com.google.common.base.Preconditions;
import com.alibaba.fastjson.JSON;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.zto.fire.common.conf.FireRocketMQConf;
import org.apache.commons.collections.map.LinkedMap;
import org.apache.commons.lang.Validate;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.MQPullConsumerScheduleService;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueByConfig;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.flink.common.serialization.TagKeyValueDeserializationSchema;
import org.apache.rocketmq.flink.util.MetricUtils;
import org.apache.rocketmq.flink.util.RetryUtil;
import org.apache.rocketmq.flink.watermark.WaterMarkForAll;
import org.apache.rocketmq.flink.watermark.WaterMarkPerQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.rocketmq.flink.RocketMQConfig.*;
import static org.apache.rocketmq.flink.RocketMQUtils.getInteger;
import static org.apache.rocketmq.flink.RocketMQUtils.getLong;

/**
 * The RocketMQSource is based on RocketMQ pull consumer mode, and provides exactly once reliability guarantees when
 * checkpoints are enabled. Otherwise, the source doesn't provide any reliability guarantees.
 */
public class RocketMQSourceWithTag<OUT> extends RichParallelSourceFunction<OUT>
        implements CheckpointedFunction, CheckpointListener, ResultTypeQueryable<OUT> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQSourceWithTag.class);

    private transient MQPullConsumerScheduleService pullConsumerScheduleService;
    private transient DefaultMQPullConsumer consumer;
    private transient DefaultMQPullConsumer bakConsumer;

    private TagKeyValueDeserializationSchema<OUT> schema;

    private RunningChecker runningChecker;

    private transient ListState<Tuple2<MessageQueue, Long>> unionOffsetStates;
    private Map<MessageQueue, Long> offsetTable;
    private Map<MessageQueue, Long> restoredOffsets;

    private List<MessageQueue> messageQueues;
    private ExecutorService executor;

    // watermark in source
    private WaterMarkPerQueue waterMarkPerQueue;
    private WaterMarkForAll waterMarkForAll;

    private ScheduledExecutorService timer;
    /**
     * Data for pending but uncommitted offsets.
     */
    private LinkedMap pendingOffsetsToCommit;
    private LinkedMap pendingTimestampToCommit;
    private Map<MessageQueue, Long> timestampTable;
    private Properties props;
    private String topic;
    private String group;

    private static final String OFFSETS_STATE_NAME = "topic-partition-offset-states-with-tags";

    private transient volatile boolean restored;
    private transient boolean enableCheckpoint;
    private volatile Object checkPointLock;

    private Meter tpsMetric;
    private String resolveType;
    private String opTypeField;

    public RocketMQSourceWithTag(TagKeyValueDeserializationSchema<OUT> schema, Properties props) {
        this.schema = schema;
        this.props = props;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        LOG.info("RocketMQ source open....");
        Validate.notEmpty(props, "Consumer properties can not be empty");

        this.topic = props.getProperty(RocketMQConfig.CONSUMER_TOPIC);
        this.group = props.getProperty(RocketMQConfig.CONSUMER_GROUP);

        Validate.notEmpty(topic, "Consumer topic can not be empty");
        Validate.notEmpty(group, "Consumer group can not be empty");

        this.enableCheckpoint =
                ((StreamingRuntimeContext) getRuntimeContext()).isCheckpointingEnabled();

        if (offsetTable == null) {
            offsetTable = new ConcurrentHashMap<>();
        }
        if (restoredOffsets == null) {
            restoredOffsets = new ConcurrentHashMap<>();
        }
        if (timestampTable == null) {
            timestampTable = new ConcurrentHashMap<>();
        }

        // use restoredOffsets to init offset table.
        if (!FireRocketMQConf.overwriteStateOffsetEnable(1)) {
            initOffsetTableFromRestoredOffsets();
        }

        if (pendingOffsetsToCommit == null) {
            pendingOffsetsToCommit = new LinkedMap();
        }
        if (pendingTimestampToCommit == null) {
            pendingTimestampToCommit = new LinkedMap();
        }
        if (checkPointLock == null) {
            checkPointLock = new ReentrantLock();
        }
        if (waterMarkPerQueue == null) {
            waterMarkPerQueue = new WaterMarkPerQueue(5000);
        }
        if (waterMarkForAll == null) {
            waterMarkForAll = new WaterMarkForAll(5000);
        }
        if (timer == null) {
            timer = Executors.newSingleThreadScheduledExecutor();
        }

        runningChecker = new RunningChecker();

        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("rmq-pull-thread-%d")
                        .build();
        executor = Executors.newCachedThreadPool(threadFactory);

        bakConsumer = pullConsumer("bak");
        consumer = pullConsumer("official");

        Counter outputCounter =
                getRuntimeContext()
                        .getMetricGroup()
                        .counter(MetricUtils.METRICS_TPS + "_counter", new SimpleCounter());
        tpsMetric =
                getRuntimeContext()
                        .getMetricGroup()
                        .meter(MetricUtils.METRICS_TPS, new MeterView(outputCounter, 60));
    }

    private DefaultMQPullConsumer pullConsumer(String type) {
        int indexOfThisSubTask = getRuntimeContext().getIndexOfThisSubtask();
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(group);
        RocketMQConfig.buildConsumerConfigs(props, consumer);

        // set unique instance name, avoid exception:
        // https://help.aliyun.com/document_detail/29646.html
        String runtimeName = ManagementFactory.getRuntimeMXBean().getName();
        String instanceName =
                RocketMQUtils.getInstanceName(
                        runtimeName,
                        topic,
                        group,
                        type,
                        String.valueOf(indexOfThisSubTask),
                        String.valueOf(System.nanoTime()));
        consumer.setInstanceName(instanceName);
        return consumer;
    }

    @Override
    public void run(SourceContext context) throws Exception {
        String tag =
                props.getProperty(RocketMQConfig.CONSUMER_TAG, RocketMQConfig.DEFAULT_CONSUMER_TAG);
        int pullBatchSize = getInteger(props, MAX_PULL_SIZE, DEFAULT_CONSUMER_BATCH_SIZE);

        final RuntimeContext ctx = getRuntimeContext();
        // The lock that guarantees that record emission and state updates are atomic,
        // from the view of taking a checkpoint.
        int taskNumber = ctx.getNumberOfParallelSubtasks();
        int taskIndex = ctx.getIndexOfThisSubtask();
        LOG.info("Source run NumberOfTotalTask={}, IndexOfThisSubTask={}, pullBatchSize={}", taskNumber, taskIndex, pullBatchSize);

        timer.scheduleAtFixedRate(
                () -> {
                    // context.emitWatermark(waterMarkPerQueue.getCurrentWatermark());
                    context.emitWatermark(waterMarkForAll.getCurrentWatermark());
                },
                5,
                5,
                TimeUnit.SECONDS);

        if (!offsetTable.isEmpty()) {
            props.setProperty(RocketMQConfig.CONSUMER_OFFSET_RESET_TO, CONSUMER_OFFSET_EARLIEST);
        }

        long recallTime = FireRocketMQConf.startFromTimestamp(1);

        bakConsumer.start();
        Collection<MessageQueue> totalQueues = bakConsumer.fetchSubscribeMessageQueues(topic);
        bakConsumer.shutdown();

        messageQueues =
                RocketMQUtils.allocate(totalQueues, taskNumber, ctx.getIndexOfThisSubtask());
        LOG.info("messageQueues run." + JSON.toJSONString(messageQueues));
        runningChecker.setRunning(true);
        List<MessageQueue> deleteMessageQueue = new ArrayList<>();
        offsetTable.keySet().forEach(mq -> {
            if (!messageQueues.contains(mq)) {
                deleteMessageQueue.add(mq);
            }
        });
        deleteMessageQueue.forEach(mq -> offsetTable.remove(mq));

        //用户自定义queue策略
        AllocateMessageQueueByConfig allocateMessageQueueByConfig = new AllocateMessageQueueByConfig();
        //指定MessageQueue
        allocateMessageQueueByConfig.setMessageQueueList(messageQueues);
        //设置consumer的负载策略
        consumer.setAllocateMessageQueueStrategy(allocateMessageQueueByConfig);
        consumer.start();

        for (MessageQueue mq : messageQueues) {
            this.executor.execute(
                    () -> {
                        RetryUtil retryUtil = new RetryUtil(runningChecker);
                        retryUtil.call(
                                () -> {
                                    while (runningChecker.isRunning()) {
                                        try {
                                            long offset = getMessageQueueOffset(mq, recallTime);
                                            PullResult pullResult =
                                                    consumer.pullBlockIfNotFound(
                                                            mq, tag, offset, pullBatchSize);

                                            boolean found = false;
                                            switch (pullResult.getPullStatus()) {
                                                case FOUND:
                                                    List<MessageExt> messages = pullResult.getMsgFoundList();
                                                    if (pullBatchSize != messages.size())
                                                        LOG.debug("Pull from rocketmq records is: {}", messages.size());
                                                    for (MessageExt msg : messages) {
                                                        byte[] tag1 = msg.getTags() != null ? msg.getTags().getBytes(StandardCharsets.UTF_8) : null;
                                                        byte[] key = msg.getKeys() != null ? msg.getKeys().getBytes(StandardCharsets.UTF_8) : null;
                                                        byte[] value = msg.getBody();
                                                        OUT data = schema.deserializeTagKeyAndValue(tag1, key, value);

                                                        // output and state update are atomic
                                                        synchronized (checkPointLock) {
                                                            context.collectWithTimestamp(data, msg.getBornTimestamp());
                                                        }
                                                    }
                                                    found = true;
                                                    break;
                                                case NO_MATCHED_MSG:
                                                    LOG.debug(
                                                            "No matched message after offset {} for queue {}",
                                                            offset,
                                                            mq);
                                                    break;
                                                case NO_NEW_MSG:
                                                    LOG.debug(
                                                            "No new message after offset {} for queue {}",
                                                            offset,
                                                            mq);
                                                    break;
                                                case OFFSET_ILLEGAL:
                                                    LOG.warn(
                                                            "Offset {} is illegal for queue {}",
                                                            offset,
                                                            mq);
                                                    break;
                                                default:
                                                    break;
                                            }

                                            synchronized (checkPointLock) {
                                                updateMessageQueueOffset(
                                                        mq, pullResult.getNextBeginOffset());
                                            }

                                            if (!found) {
                                                retryUtil.waitForMs(
                                                        RocketMQConfig
                                                                .DEFAULT_CONSUMER_DELAY_WHEN_MESSAGE_NOT_FOUND);
                                            }
                                            // 不报错了，重置次数
                                            retryUtil.setRetries(1);
                                        } catch (Exception e) {
                                            LOG.error("messageQueues " + mq + " run error.", e);
                                            throw new RuntimeException(e);
                                        }
                                    }
                                    return true;
                                },
                                "RuntimeException");
                    });
        }

        awaitTermination();
    }

    private void awaitTermination() throws InterruptedException {
        while (runningChecker.isRunning()) {
            Thread.sleep(50);
        }
    }

    private long getMessageQueueOffset(MessageQueue mq, Long recallTime) throws MQClientException {
        Long offset = offsetTable.get(mq);
        // restoredOffsets(unionOffsetStates) is the restored global union state;
        // should only snapshot mqs that actually belong to us
        if (offset == null) {
            if (recallTime > 0) {
                props.setProperty(RocketMQConfig.CONSUMER_OFFSET_RESET_TO, CONSUMER_OFFSET_TIMESTAMP);
                props.setProperty(RocketMQConfig.CONSUMER_OFFSET_FROM_TIMESTAMP, String.valueOf(recallTime));
            }

            // fetchConsumeOffset from broker
            offset = consumer.fetchConsumeOffset(mq, false);
            int retryCount = 1;
            while (offset < 0 && retryCount <= 3) {
                offset = consumer.fetchConsumeOffset(mq, false);
                retryCount += 1;
                LOG.error("offset-> fetchConsumeOffset 返回值={} 第{}次重试.", offset, retryCount);
            }

            LOG.info("队列：{}-{}, 回溯时间：{}, 当前位点：{}", mq.getBrokerName(), mq.getQueueId(), recallTime, offset);
            if (!restored || offset < 0 || recallTime > 0) {
                String initialOffset = props.getProperty(RocketMQConfig.CONSUMER_OFFSET_RESET_TO, CONSUMER_OFFSET_LATEST);

                switch (initialOffset) {
                    case CONSUMER_OFFSET_EARLIEST:
                        offset = consumer.minOffset(mq);
                        LOG.info("队列：{}-{}, 回溯时间：{}, 最早位点：{}", mq.getBrokerName(), mq.getQueueId(), recallTime, offset);
                        break;
                    case CONSUMER_OFFSET_LATEST:
                        offset = consumer.maxOffset(mq);
                        LOG.info("队列：{}-{}, 回溯时间：{}, 最新位点：{}", mq.getBrokerName(), mq.getQueueId(), recallTime, offset);
                        break;
                    case CONSUMER_OFFSET_TIMESTAMP:
                        offset = consumer.searchOffset(mq,
                                getLong(
                                        props,
                                        RocketMQConfig.CONSUMER_OFFSET_FROM_TIMESTAMP,
                                        System.currentTimeMillis()));
                        LOG.info("队列：{}-{}, 回溯时间：{}, 回溯位点：{}", mq.getBrokerName(), mq.getQueueId(), recallTime, offset);
                        break;
                    default:
                        throw new IllegalArgumentException(
                                "Unknown value for CONSUMER_OFFSET_RESET_TO.");
                }
            }
        }

        offsetTable.put(mq, offset);
        return offsetTable.get(mq);
    }

    private void putMessageQueueOffset(MessageQueue mq, long offset) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        offsetTable.put(mq, offset);
        if (!enableCheckpoint) {
            consumer.updateConsumeOffset(mq, offset);
//            consumer.getOffsetStore().updateConsumeOffsetToBroker(mq,offset,true);
        }
    }

    private void updateMessageQueueOffset(MessageQueue mq, long offset) throws MQClientException {
        offsetTable.put(mq, offset);
        if (!enableCheckpoint) {
            consumer.updateConsumeOffset(mq, offset);
        }
    }

    @Override
    public void cancel() {
        LOG.debug("cancel ...");
        runningChecker.setRunning(false);

        if (pullConsumerScheduleService != null) {
            pullConsumerScheduleService.shutdown();
        }
        if (offsetTable != null) {
            offsetTable.clear();
        }
        if (restoredOffsets != null) {
            restoredOffsets.clear();
        }
        if (pendingOffsetsToCommit != null) {
            pendingOffsetsToCommit.clear();
        }
    }

    @Override
    public void close() throws Exception {
        LOG.debug("close ...");
        // pretty much the same logic as cancelling
        try {
            cancel();
        } finally {
            super.close();
        }
    }

    public void initOffsetTableFromRestoredOffsets() {
        Preconditions.checkNotNull(restoredOffsets, "restoredOffsets can't be null");
        restoredOffsets.forEach(
                (mq, offset) -> {
                    if (!offsetTable.containsKey(mq) || offsetTable.get(mq) < offset) {
                        offsetTable.put(mq, offset);
                    }
                });
        LOG.info("init offset table from restoredOffsets successful.{}", offsetTable);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // called when a snapshot for a checkpoint is requested
        LOG.info("Snapshotting state {} ...", context.getCheckpointId());
        if (!runningChecker.isRunning()) {
            LOG.info("snapshotState() called on closed source; returning null.");
            return;
        }

        RetryUtil retryUtil = new RetryUtil(runningChecker);
        retryUtil.setMaxAttempts(2);
        // Discovery topic Route change when snapshot
        retryUtil.call(() -> {
                    Collection<MessageQueue> totalQueues =
                            consumer.fetchSubscribeMessageQueues(topic);
                    int taskNumber = getRuntimeContext().getNumberOfParallelSubtasks();
                    int taskIndex = getRuntimeContext().getIndexOfThisSubtask();
                    List<MessageQueue> newQueues = RocketMQUtils.allocate(totalQueues, taskNumber, taskIndex);
                    Collections.sort(newQueues);
                    LOG.info("Old messageQueues：" + JSON.toJSONString(messageQueues));
                    LOG.info("New messageQueues：" + JSON.toJSONString(newQueues));

                    if (!messageQueues.equals(newQueues)) {
                        String logInfo = "Topic " + this.topic + " route has changed [" + (newQueues.size() - messageQueues.size()) + "], old messageQueues[" + messageQueues + "], new messageQueues[" + newQueues + "]";
                        LOG.error(logInfo);
                        throw new RuntimeException(logInfo);
                    }
                    return true;
                },
                "RuntimeException due to topic route changed");

        unionOffsetStates.clear();
        HashMap<MessageQueue, Long> currentOffsets = new HashMap<>(offsetTable.size());
        HashMap<MessageQueue, Long> currentTimestamp = new HashMap<>(offsetTable.size());
        for (Map.Entry<MessageQueue, Long> entry : offsetTable.entrySet()) {
            unionOffsetStates.add(Tuple2.of(entry.getKey(), entry.getValue()));
            currentOffsets.put(entry.getKey(), entry.getValue());
            currentTimestamp.put(entry.getKey(), timestampTable.get(entry.getKey()));
        }

        pendingOffsetsToCommit.put(context.getCheckpointId(), currentOffsets);
        pendingTimestampToCommit.put(context.getCheckpointId(), currentTimestamp);
        LOG.info("Snapshotted state, last processed offsets: {}, checkpoint id: {}, timestamp: {}",
                offsetTable, context.getCheckpointId(), context.getCheckpointTimestamp());
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        LOG.info("initialize State ...");

        this.unionOffsetStates = context.getOperatorStateStore().getUnionListState(
                                new ListStateDescriptor<>(
                                        OFFSETS_STATE_NAME,
                                        TypeInformation.of(
                                                new TypeHint<Tuple2<MessageQueue, Long>>() {
                                                })));
        this.restored = context.isRestored();

        if (restored) {
            if (restoredOffsets == null) {
                restoredOffsets = new ConcurrentHashMap<>();
            }
            for (Tuple2<MessageQueue, Long> mqOffsets : unionOffsetStates.get()) {
                if (!restoredOffsets.containsKey(mqOffsets.f0)
                        || restoredOffsets.get(mqOffsets.f0) < mqOffsets.f1) {
                    restoredOffsets.put(mqOffsets.f0, mqOffsets.f1);
                }
            }
            LOG.info(
                    "Setting restore state in the consumer. Using the following offsets: {}",
                    restoredOffsets);
        } else {
            LOG.info("No restore state for the consumer.");
        }
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return schema.getProducedType();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // callback when checkpoint complete
        if (!runningChecker.isRunning()) {
            LOG.info("notifyCheckpointComplete() called on closed source; returning null.");
            return;
        }

        final int posInMap = pendingOffsetsToCommit.indexOf(checkpointId);
        if (posInMap == -1) {
            LOG.warn("Received confirmation for unknown checkpoint id {}", checkpointId);
            return;
        }

        Map<MessageQueue, Long> offsets =
                (Map<MessageQueue, Long>) pendingOffsetsToCommit.remove(posInMap);

        // remove older checkpoints in map
        for (int i = 0; i < posInMap; i++) {
            pendingOffsetsToCommit.remove(0);
        }

        if (offsets == null || offsets.size() == 0) {
            LOG.debug("Checkpoint state was empty.");
            return;
        }

        for (Map.Entry<MessageQueue, Long> entry : offsets.entrySet()) {
            consumer.updateConsumeOffset(entry.getKey(), entry.getValue());
        }

        Map<MessageQueue, Long> timestamp = new HashMap<>();
        final int posInMap1 = pendingTimestampToCommit.indexOf(checkpointId);
        if (posInMap1 != -1) {
            timestamp = (Map<MessageQueue, Long>) pendingTimestampToCommit.remove(posInMap);
            // remove older checkpoints in map
            for (int i = 0; i < posInMap1; i++) {
                pendingTimestampToCommit.remove(0);
            }
        }
    }
}
