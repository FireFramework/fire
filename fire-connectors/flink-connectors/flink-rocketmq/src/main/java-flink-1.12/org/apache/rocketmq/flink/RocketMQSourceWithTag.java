/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.rocketmq.flink;

import com.esotericsoftware.minlog.Log;
import org.apache.commons.collections.map.LinkedMap;
import org.apache.commons.lang.Validate;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.MQPullConsumerScheduleService;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.flink.common.serialization.TagKeyValueDeserializationSchema;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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
    private DefaultMQPullConsumer consumer;

    private TagKeyValueDeserializationSchema<OUT> schema;

    private RunningChecker runningChecker;

    private transient ListState<Tuple2<MessageQueue, Long>> unionOffsetStates;
    private Map<MessageQueue, Long> offsetTable;
    private Map<MessageQueue, Long> restoredOffsets;
    /** Data for pending but uncommitted offsets. */
    private LinkedMap pendingOffsetsToCommit;

    private Properties props;
    private String topic;
    private String group;

    private static final String OFFSETS_STATE_NAME = "topic-partition-offset-states-with-tags";

    private transient volatile boolean restored;
    private transient boolean enableCheckpoint;

    public RocketMQSourceWithTag(TagKeyValueDeserializationSchema<OUT> schema, Properties props) {
        this.schema = schema;
        this.props = props;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        LOG.debug("source open....");
        Validate.notEmpty(props, "Consumer properties can not be empty");
        Validate.notNull(schema, "TagKeyValueDeserializationSchema can not be null");

        this.topic = props.getProperty(RocketMQConfig.CONSUMER_TOPIC);
        this.group = props.getProperty(RocketMQConfig.CONSUMER_GROUP);

        Validate.notEmpty(topic, "Consumer topic can not be empty");
        Validate.notEmpty(group, "Consumer group can not be empty");

        this.enableCheckpoint = ((StreamingRuntimeContext) getRuntimeContext()).isCheckpointingEnabled();

        if (offsetTable == null) {
            offsetTable = new ConcurrentHashMap<>();
        }
        if (restoredOffsets == null) {
            restoredOffsets = new ConcurrentHashMap<>();
        }
        if (pendingOffsetsToCommit == null) {
            pendingOffsetsToCommit = new LinkedMap();
        }

        runningChecker = new RunningChecker();

        //Wait for lite pull consumer
        pullConsumerScheduleService = new MQPullConsumerScheduleService(group, RocketMQConfig.buildAclRPCHook(props));
        consumer = pullConsumerScheduleService.getDefaultMQPullConsumer();

        consumer.setInstanceName(getRuntimeContext().getIndexOfThisSubtask() + "_" + UUID.randomUUID());
        RocketMQConfig.buildConsumerConfigs(props, consumer);
    }

    @Override
    public void run(SourceContext context) throws Exception {
        LOG.debug("source run....");
        // The lock that guarantees that record emission and state updates are atomic,
        // from the view of taking a checkpoint.
        final Object lock = context.getCheckpointLock();

        int delayWhenMessageNotFound = getInteger(props, RocketMQConfig.CONSUMER_DELAY_WHEN_MESSAGE_NOT_FOUND,
                RocketMQConfig.DEFAULT_CONSUMER_DELAY_WHEN_MESSAGE_NOT_FOUND);

        String tag = props.getProperty(RocketMQConfig.CONSUMER_TAG, RocketMQConfig.DEFAULT_CONSUMER_TAG);

        int pullPoolSize = getInteger(props, RocketMQConfig.CONSUMER_PULL_POOL_SIZE,
                RocketMQConfig.DEFAULT_CONSUMER_PULL_POOL_SIZE);

        int pullBatchSize = getInteger(props, RocketMQConfig.CONSUMER_BATCH_SIZE,
                RocketMQConfig.DEFAULT_CONSUMER_BATCH_SIZE);

        pullConsumerScheduleService.setPullThreadNums(pullPoolSize);
        pullConsumerScheduleService.registerPullTaskCallback(topic, (mq, pullTaskContext) -> {
            try {
                long offset = getMessageQueueOffset(mq);
                if (offset < 0) {
                    return;
                }
                Log.debug("Current pullBatchSize is: " + pullBatchSize);
                PullResult pullResult = consumer.pull(mq, tag, offset, pullBatchSize);
                boolean found = false;
                switch (pullResult.getPullStatus()) {
                    case FOUND:
                        List<MessageExt> messages = pullResult.getMsgFoundList();
                        if (pullBatchSize != messages.size()) LOG.debug("Pull from rocketmq records is: {}", messages.size());
                        for (MessageExt msg : messages) {
                            byte[] tag1 = msg.getTags() != null ? msg.getTags().getBytes(StandardCharsets.UTF_8) : null;
                            byte[] key = msg.getKeys() != null ? msg.getKeys().getBytes(StandardCharsets.UTF_8) : null;
                            byte[] value = msg.getBody();
                            OUT data = schema.deserializeTagKeyAndValue(tag1, key, value);

                            // output and state update are atomic
                            synchronized (lock) {
                                context.collectWithTimestamp(data, msg.getBornTimestamp());
                            }
                        }
                        found = true;
                        break;
                    case NO_MATCHED_MSG:
                        LOG.debug("No matched message after offset {} for queue {}", offset, mq);
                        break;
                    case NO_NEW_MSG:
                        break;
                    case OFFSET_ILLEGAL:
                        LOG.warn("Offset {} is illegal for queue {}", offset, mq);
                        break;
                    default:
                        break;
                }

                synchronized (lock) {
                    putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
                }

                if (found) {
                    pullTaskContext.setPullNextDelayTimeMillis(0); // no delay when messages were found
                } else {
                    pullTaskContext.setPullNextDelayTimeMillis(delayWhenMessageNotFound);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        try {
            pullConsumerScheduleService.start();
        } catch (MQClientException e) {
            throw new RuntimeException(e);
        }
        runningChecker.setRunning(true);
        awaitTermination();
    }

    private void awaitTermination() throws InterruptedException {
        while (runningChecker.isRunning()) {
            Thread.sleep(50);
        }
    }

    private long getMessageQueueOffset(MessageQueue mq) throws MQClientException {
        Long offset = offsetTable.get(mq);

        if (offset == null) {
            LOG.debug("从状态中获取Offset列表为空，将从server端获取offset列表");
            offset = consumer.fetchConsumeOffset(mq, true);

            int retryCount = 1;
            while (offset < 0 && retryCount <= 3) {
                offset = consumer.fetchConsumeOffset(mq, true);
                retryCount += 1;
                LOG.error("offset-> fetchConsumeOffset 返回值=" + offset + " 第" + retryCount + "次重试.");
            }

            if (offset < 0) {
                String initialOffset = props.getProperty(RocketMQConfig.CONSUMER_OFFSET_RESET_TO, CONSUMER_OFFSET_LATEST);
                Log.error("重试获取offset 5次仍失败，将强制重置offset=" + initialOffset);
                switch (initialOffset) {
                    case CONSUMER_OFFSET_EARLIEST:
                        offset = consumer.minOffset(mq);
                        break;
                    case CONSUMER_OFFSET_LATEST:
                        offset = consumer.maxOffset(mq);
                        break;
                    case CONSUMER_OFFSET_TIMESTAMP:
                        offset = consumer.searchOffset(mq, getLong(props,
                                RocketMQConfig.CONSUMER_OFFSET_FROM_TIMESTAMP, System.currentTimeMillis()));
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown value for CONSUMER_OFFSET_RESET_TO.");
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

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // called when a snapshot for a checkpoint is requested

        if (!runningChecker.isRunning()) {
            LOG.debug("snapshotState() called on closed source; returning null.");
            return;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Snapshotting state {} ...", context.getCheckpointId());
        }

        unionOffsetStates.clear();

        HashMap<MessageQueue, Long> currentOffsets = new HashMap<>(offsetTable.size());

        // remove the unassigned queues in order to avoid read the wrong offset when the source restart
        Set<MessageQueue> assignedQueues = consumer.fetchMessageQueuesInBalance(topic);
        offsetTable.entrySet().removeIf(item -> !assignedQueues.contains(item.getKey()));

        for (Map.Entry<MessageQueue, Long> entry : offsetTable.entrySet()) {
            unionOffsetStates.add(Tuple2.of(entry.getKey(), entry.getValue()));
            currentOffsets.put(entry.getKey(), entry.getValue());
        }

        pendingOffsetsToCommit.put(context.getCheckpointId(), currentOffsets);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Snapshotted state, last processed offsets: {}, checkpoint id: {}, timestamp: {}",
                    offsetTable, context.getCheckpointId(), context.getCheckpointTimestamp());
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // called every time the user-defined function is initialized,
        // be that when the function is first initialized or be that
        // when the function is actually recovering from an earlier checkpoint.
        // Given this, initializeState() is not only the place where different types of state are initialized,
        // but also where state recovery logic is included.
        LOG.debug("initialize State ...");

        this.unionOffsetStates = context.getOperatorStateStore().getUnionListState(new ListStateDescriptor<>(
                OFFSETS_STATE_NAME, TypeInformation.of(new TypeHint<Tuple2<MessageQueue, Long>>() {})));
        this.restored = context.isRestored();
        if (restored) {
            if (restoredOffsets == null) {
                restoredOffsets = new ConcurrentHashMap<>();
            }
            if (offsetTable == null) {
                offsetTable = new ConcurrentHashMap<>();
            }
            for (Tuple2<MessageQueue, Long> mqOffsets : unionOffsetStates.get()) {
                if (!restoredOffsets.containsKey(mqOffsets.f0) || restoredOffsets.get(mqOffsets.f0) < mqOffsets.f1) {
                    restoredOffsets.put(mqOffsets.f0, mqOffsets.f1);
                    // 将状态中的offset列表在初始化时直接塞给offsetTable
                    offsetTable.put(mqOffsets.f0, mqOffsets.f1);
                }
            }
            LOG.info("Setting restore state in the consumer. Using the following offsets: {}", restoredOffsets);
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
            LOG.debug("notifyCheckpointComplete() called on closed source; returning null.");
            return;
        }

        final int posInMap = pendingOffsetsToCommit.indexOf(checkpointId);
        if (posInMap == -1) {
            LOG.warn("Received confirmation for unknown checkpoint id {}", checkpointId);
            return;
        }

        Map<MessageQueue, Long> offsets = (Map<MessageQueue, Long>) pendingOffsetsToCommit.remove(posInMap);

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

    }
}
