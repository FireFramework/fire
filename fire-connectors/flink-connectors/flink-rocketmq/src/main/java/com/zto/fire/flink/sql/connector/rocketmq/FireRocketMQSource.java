package com.zto.fire.flink.sql.connector.rocketmq;


import org.apache.commons.collections.map.LinkedMap;
import org.apache.commons.lang3.Validate;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.CheckpointListener;
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
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.shaded.curator4.org.apache.curator.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.Collector;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.flink.RocketMQConfig;
import org.apache.rocketmq.flink.RocketMQUtils;
import org.apache.rocketmq.flink.RunningChecker;
import org.apache.rocketmq.flink.util.MetricUtils;
import org.apache.rocketmq.flink.util.RetryUtil;
import org.apache.rocketmq.flink.watermark.WaterMarkForAll;
import org.apache.rocketmq.flink.watermark.WaterMarkPerQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.*;

/**
 * 描述：{@link org.apache.rocketmq.client.consumer.DefaultMQPullConsumer} 弃用，
 * 推荐使用 {@link DefaultLitePullConsumer}
 * 时间：3/11/2021 上午9:31
 */
public class FireRocketMQSource<OUT> extends RichParallelSourceFunction<OUT>
        implements CheckpointedFunction, CheckpointListener, ResultTypeQueryable<OUT> {
    private static final Logger LOG = LoggerFactory.getLogger(FireRocketMQSource.class);
    private static final long serialVersionUID = 1L;
    // state name
    private static final String OFFSETS_STATE_NAME = "topic-partition-offset-states";
    private transient ListState<Tuple2<MessageQueue, Long>> unionOffsetStates;
    private LinkedMap pendingOffsetsToCommit = new LinkedMap();
    private RunningChecker runningChecker;
    private transient volatile boolean restored;
    private transient boolean enableCheckpoint;
    private RocketMQCollector rocketMQCollector;
    private List<MessageQueue> assignQueues;
    private ExecutorService executor;
    private DefaultLitePullConsumer consumer;
    private final RocketMQDeserializationSchema<OUT> deserializer;
    private final Properties props;
    private String topic;
    private String group;
    private final Map<MessageQueue, Long> offsetTable = new ConcurrentHashMap<>();
    private ScheduledExecutorService timer;
    // watermark in source
    private WaterMarkPerQueue waterMarkPerQueue;
    private WaterMarkForAll waterMarkForAll;
    private Meter tpsMetric;

    private boolean sendAllTag;
    private Set<String> tagSet;

    public FireRocketMQSource(RocketMQDeserializationSchema<OUT> deserializer, Properties props) {
        this.deserializer = deserializer;
        this.props = props;
        String tags = props.getProperty(RocketMQConfig.CONSUMER_TAG);
        // 如果没传tag 或者 tag 为 *，则表示发送所有数据
        if (tags == null || "*".equals(tags.trim())) {
            sendAllTag = true;
        } else {
            tagSet = new HashSet<>();
            // 可以指定过滤多个tag，使用逗号分隔  如：accuracy1,accuracy2
            String[] split = tags.split(",");
            for (String s : split) {
                tagSet.add(s.trim());
            }
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        LOG.debug("source run ......");

        Validate.notEmpty(props, "Consumer properties can not be empty");

        runningChecker = new RunningChecker();
        runningChecker.setRunning(true);
        this.topic = props.getProperty(RocketMQConfig.CONSUMER_TOPIC);
        this.group = props.getProperty(RocketMQConfig.CONSUMER_GROUP);

        Validate.notEmpty(topic, "Consumer topic can not be empty");
        Validate.notEmpty(group, "Consumer group can not be empty");
        this.enableCheckpoint = ((StreamingRuntimeContext) getRuntimeContext()).isCheckpointingEnabled();

        rocketMQCollector = new RocketMQCollector();

        waterMarkPerQueue = new WaterMarkPerQueue(5000);

        waterMarkForAll = new WaterMarkForAll(5000);

        Counter outputCounter = getRuntimeContext().getMetricGroup()
                .counter(MetricUtils.METRICS_TPS + "_counter", new SimpleCounter());
        tpsMetric = getRuntimeContext().getMetricGroup()
                .meter(MetricUtils.METRICS_TPS, new MeterView(outputCounter, 60));
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setDaemon(true).setNameFormat("rmq-pull-thread-%d").build();
        executor = Executors.newCachedThreadPool(threadFactory);

        timer = Executors.newSingleThreadScheduledExecutor();
        // 初始化consumer 并分配队列
        startConsumer();
    }

    @Override
    public void run(SourceContext<OUT> context) throws Exception {
        // 只有当consumer分配到queue时，才进行消费
        if (!assignQueues.isEmpty()) {
            timer.scheduleAtFixedRate(() -> {
                context.emitWatermark(waterMarkPerQueue.getCurrentWatermark());
                context.emitWatermark(waterMarkForAll.getCurrentWatermark());
            }, 5, 5, TimeUnit.SECONDS);

            this.executor.execute(() -> RetryUtil.call(() -> {
                while (runningChecker.isRunning()) {
                    List<MessageExt> messages = consumer.poll();
                    for (MessageExt msg : messages) {
                        deserializer.deserialize(msg, rocketMQCollector);
                        // get record from collector and use sourceContext emit it to down task
                        Queue<OUT> records = rocketMQCollector.getRecords();
                        synchronized (context.getCheckpointLock()) {
                            OUT record;
                            while ((record = records.poll()) != null) {
                                String tags = msg.getTags();
                                // 如果是发送所有消息（未指定tag或者tag为*） 或者 指定tag了，并且消息的tag在指定的tag之内，则下发消息
                                if (sendAllTag || tagSet.contains(tags)) {
                                    context.collectWithTimestamp(record, msg.getBornTimestamp());
                                }
                            }
                            // record offset to offset table
                            recordBrokerOffset(msg);

                            // update max eventTime per queue
                            waterMarkPerQueue.extractTimestamp(buildMessageQueue(msg), msg.getBornTimestamp());
                            waterMarkForAll.extractTimestamp(msg.getBornTimestamp());
                            tpsMetric.markEvent();
                        }
                    }
                }
                return true;
            }, "RuntimeException"));
        }

        awaitTermination();
    }

    private void awaitTermination() throws InterruptedException {
        while (runningChecker.isRunning()) {
            Thread.sleep(50);
        }
    }

    @Override
    public void cancel() {
        LOG.debug("cancel ...");
        runningChecker.setRunning(false);

        if (consumer != null) {
            consumer.shutdown();
        }

        if (offsetTable != null) {
            offsetTable.clear();
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
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
            // MessageQueue中记录的 offset 会比此消息实际offset少1，所以在更新的时候需要 + 1
            LOG.info("update {} to {}", entry.getKey(), entry.getValue() + 1);
//            consumer.commitSync();
//            consumer.getOffsetStore().updateConsumeOffsetToBroker(entry.getKey(), entry.getValue(), true);
            consumer.getOffsetStore().updateOffset(entry.getKey(), entry.getValue() + 1, false);
        }
    }


    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // called when a snapshot for a checkpoint is requested
        LOG.info("Snapshotting state {} ...", context.getCheckpointId());
        if (!runningChecker.isRunning()) {
            LOG.info("snapshotState() called on closed source; returning null.");
            return;
        }

        // Discovery topic Route change when snapshot
        // 每次进行snapshot时，进行rocket-mq queue检查，
        // 如果有新增的queue，则进行报错，需要进行重启重新分配queue。

        // todo 这里可以当发现queue增加后，直接进行分配消费
        RetryUtil.call(() -> {
            List<MessageQueue> newQueues = getAssignQueues();
            Collections.sort(newQueues);
            if (!this.assignQueues.equals(newQueues)) {
                throw new RuntimeException("topic route changed, this task need to be restarted!");
            }
            return true;
        }, "RuntimeException due to topic route changed");


        unionOffsetStates.clear();
        Map<MessageQueue, Long> currentOffset = new HashMap<>(assignQueues.size());
        for (Map.Entry<MessageQueue, Long> entry : offsetTable.entrySet()) {
            LOG.info("snapshot {}, offset {}", entry.getKey(), entry.getValue());
            unionOffsetStates.add(Tuple2.of(entry.getKey(), entry.getValue()));
            currentOffset.put(entry.getKey(), entry.getValue());
        }
        pendingOffsetsToCommit.put(context.getCheckpointId(), currentOffset);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        LOG.info("initialize State ...");
        unionOffsetStates = context.getOperatorStateStore().getUnionListState(new ListStateDescriptor<>(
                OFFSETS_STATE_NAME, TypeInformation.of(new TypeHint<Tuple2<MessageQueue, Long>>() {
        })));
        this.restored = context.isRestored();
        if (restored) {
            unionOffsetStates.get().forEach(t -> offsetTable.put(t.f0, t.f1));
            LOG.info("Restore from state, {}", offsetTable);
        } else {
            LOG.info("No restored from state ......");
        }
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return deserializer.getProducedType();
    }

    private void recordBrokerOffset(MessageExt message) {
        MessageQueue mq = buildMessageQueue(message);
        long queueOffset = message.getQueueOffset();
        offsetTable.put(mq, queueOffset);
        // 如果没有启用chk，并且没有启用自动提交，那么每次要提交offset
        if (!enableCheckpoint && !consumer.isAutoCommit()) {
            consumer.commitSync();
        }
    }

    private MessageQueue buildMessageQueue(MessageExt message) {
        String topic = message.getTopic();
        String brokerName = message.getBrokerName();
        int queueId = message.getQueueId();
        return new MessageQueue(topic, brokerName, queueId);
    }

    private void startConsumer() throws MQClientException {
        LOG.info("consumer start ");
        this.consumer = new DefaultLitePullConsumer(this.group, RocketMQConfig.buildAclRPCHook(props));
        String nameServers = props.getProperty(RocketMQConfig.NAME_SERVER_ADDR);
        Validate.notEmpty(nameServers);
        this.consumer.setNamesrvAddr(nameServers);
        this.consumer.setPollNameServerInterval(RocketMQUtils.getInteger(props,
                RocketMQConfig.NAME_SERVER_POLL_INTERVAL, RocketMQConfig.DEFAULT_NAME_SERVER_POLL_INTERVAL));
        this.consumer.setHeartbeatBrokerInterval(RocketMQUtils.getInteger(props,
                RocketMQConfig.BROKER_HEART_BEAT_INTERVAL, RocketMQConfig.DEFAULT_BROKER_HEART_BEAT_INTERVAL));
        String runtimeName = ManagementFactory.getRuntimeMXBean().getName();
        int indexOfThisSubTask = getRuntimeContext().getIndexOfThisSubtask();
        String instanceName = RocketMQUtils.getInstanceName(runtimeName, topic, group,
                String.valueOf(indexOfThisSubTask), String.valueOf(System.nanoTime()));
        this.consumer.setInstanceName(instanceName);
        boolean autoCommit = RocketMQUtils.getBoolean(props, RocketMQConfig.OFFSET_AUTO_COMMIT, false);
        this.consumer.setAutoCommit(autoCommit);
        this.consumer.start();
        this.assignQueues = getAssignQueues();
        // 如果此task分配的队列为空，则停止此consumer
        if (!this.assignQueues.isEmpty()) {
            this.consumer.assign(this.assignQueues);
            // 从offsetTable中移除不在本task 中分配的队列，做snapshot，
            // 并从恢复的offset中，seek到上次的offset
            removeUnAssignQueues();

            // 每个MessageQueue指定offset消费
            perQueueSeekToSpecialOffset();
        } else {
            this.offsetTable.clear();
        }
    }

    private List<MessageQueue> getAssignQueues() throws MQClientException {
        final RuntimeContext ctx = getRuntimeContext();
        int taskNumber = ctx.getNumberOfParallelSubtasks();
        int taskIndex = ctx.getIndexOfThisSubtask();
        Collection<MessageQueue> totalQueues = this.consumer.fetchMessageQueues(this.topic);
        List<MessageQueue> shouldAssignQueues = RocketMQUtils.allocate(totalQueues, taskNumber, taskIndex);
        return shouldAssignQueues;
    }

    // 从offsetTable中移除此consumer未消费的信息
    private void removeUnAssignQueues() throws MQClientException {
        // offset table 开始从状态恢复，保存的是全部queue的信息，移除多余的
        this.offsetTable.forEach((k, v) -> {
            if (!this.assignQueues.contains(k)) {
                this.offsetTable.remove(k);
            }
        });
    }

    // 根据不同策略，指定不同offset
    private void perQueueSeekToSpecialOffset() throws MQClientException {
        for (MessageQueue mq : this.assignQueues) {
            if (this.offsetTable.containsKey(mq)) {
                long nextOffset = this.offsetTable.get(mq) + 1;
                LOG.info("consumer seek {} from state, offset {}", mq, nextOffset);
                this.consumer.seek(mq, nextOffset);
            } else {
                // 1、如果是从状态恢复，但是找不到，那么这个offset 就是新加入的
                // 2、 另一种可能是，当前这种策略，每个offset table中只保留当前TaskManager的 MQ offset，
                // 如果在这段时间，某个mq没有数据尽量，checkpoint时没有此mq的数据，这时也找不到，如果设置从seekToBegin就会有问题
                // 目前在生产环境，这种情况应该不会出现。
                if (this.restored) {
                    this.consumer.seekToBegin(mq);
                    LOG.info("restore but not found in offsetTable, seek {} to begin", mq);
                } else {
                    // 不是restored，那么就是直接重启，根据提供的策略选择offset
                    String offsetFrom = props.getProperty(RocketMQConfig.CONSUMER_OFFSET_RESET_TO, "latest");
//                    Offset initialOffsetFrom = Offset.valueOf(offsetFrom);
                    switch (offsetFrom) {
                        case "earliest-offset":
                            this.consumer.seekToBegin(mq);
                            LOG.info("{} seek to begin ......", mq);
                            break;
                        case "latest-offset":
                            this.consumer.seekToEnd(mq);
                            LOG.info("{} seek to end ......", mq);
                            break;
                        case "store":
                            long storedOffset = this.consumer.getOffsetStore().readOffset(mq, ReadOffsetType.READ_FROM_STORE);
                            LOG.info("{} seek to stored offset {}", mq, storedOffset);
                            this.consumer.seek(mq, storedOffset);
                            break;
                        default:
                            throw new RuntimeException(String.format("Not supported Offset [%s]", offsetFrom));
                    }
                }
            }
        }
    }

    // 用来保存值
    private class RocketMQCollector implements Collector<OUT> {
        private final Queue<OUT> records = new ArrayDeque<>();
        private boolean endOfStreamSignalled = false;

        @Override
        public void collect(OUT record) {
            // do not emit subsequent elements if the end of the stream reached
            if (endOfStreamSignalled || deserializer.isEndOfStream(record)) {
                endOfStreamSignalled = true;
                return;
            }
            records.add(record);
        }

        public Queue<OUT> getRecords() {
            return records;
        }

        public boolean isEndOfStreamSignalled() {
            return endOfStreamSignalled;
        }

        public void setEndOfStreamSignalled(boolean endOfStreamSignalled) {
            this.endOfStreamSignalled = endOfStreamSignalled;
        }

        @Override
        public void close() {

        }
    }
}
