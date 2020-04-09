package com.zto.fire.flink.core.ext.functions;

import com.zto.fire.common.util.ValueUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.*;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Collector;

import java.io.File;
import java.io.Serializable;
import java.util.List;

/**
 * 增强的MapFunction
 *
 * @param <IN>  输入数据类型
 * @param <OUT> 输出数据类型（map后）
 * @author ChengLong 2020-4-9 09:39:55
 */
public abstract class FireMapFunction<IN, OUT> extends AbstractRichFunction implements MapFunction<IN, OUT>, MapPartitionFunction<IN, OUT>, FlatMapFunction<IN, OUT> {

    @Override
    public void open(Configuration parameters) throws Exception {

    }

    /**
     * map操作需复写该方法
     */
    @Override
    public OUT map(IN value) throws Exception {
        return null;
    }

    /**
     * mapPartition操作需复写该方法
     */
    @Override
    public void mapPartition(Iterable<IN> values, Collector<OUT> out) throws Exception {

    }

    /**
     * flatMap操作需复写该方法
     */
    @Override
    public void flatMap(IN value, Collector<OUT> out) throws Exception {
    }

    protected String getTaskNameWithSubtasks() {
        return this.getRuntimeContext().getTaskNameWithSubtasks();
    }

    protected String getTaskName() {
        return this.getRuntimeContext().getTaskName();
    }

    protected <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) {
        return this.getRuntimeContext().getReducingState(stateProperties);
    }

    protected int getMaxNumberOfParallelSubtasks() {
        return this.getRuntimeContext().getMaxNumberOfParallelSubtasks();
    }

    protected <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties) {
        return this.getRuntimeContext().getMapState(stateProperties);
    }

    protected <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
        return this.getRuntimeContext().getListState(stateProperties);
    }

    protected int getIndexOfThisSubtask() {
        return this.getRuntimeContext().getIndexOfThisSubtask();
    }

    protected MetricGroup getMetricGroup() {
        return this.getRuntimeContext().getMetricGroup();
    }

    /**
     * 根据名称获取对应的Histogram
     *
     * @param name Histogram名称
     */
    protected Histogram getHistogram(String name) {
        ValueUtils.requireNonNull(name, "Histogram名称不能为空");
        return this.getRuntimeContext().getHistogram(name);
    }

    /**
     * 获取指定的广播变量
     *
     * @param name 广播变量名称
     * @param <T>  广播变量类型
     * @return 广播变量集合
     */
    protected <T> List<T> getBroadcastVariable(String name) {
        ValueUtils.requireNonNull(name, "广播变量名称不能为空！");
        return this.getRuntimeContext().getBroadcastVariable(name);
    }

    protected <T, C> C getBroadcastVariableWithInitializer(String name, BroadcastVariableInitializer<T, C> initializer) {
        return this.getRuntimeContext().getBroadcastVariableWithInitializer(name, initializer);
    }

    protected int getAttemptNumber() {
        return this.getRuntimeContext().getAttemptNumber();
    }

    protected <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(AggregatingStateDescriptor<IN, ACC, OUT> stateProperties) {
        return this.getRuntimeContext().getAggregatingState(stateProperties);
    }

    protected <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
        return this.getRuntimeContext().getState(stateProperties);
    }

    /**
     * 获取配置信息
     */
    protected ExecutionConfig getExecutionConfig() {
        return this.getRuntimeContext().getExecutionConfig();
    }

    /**
     * 获取分布式缓存对象
     */
    protected DistributedCache getDistributedCache() {
        return this.getRuntimeContext().getDistributedCache();
    }

    /**
     * 将值添加到指定的累加器中
     *
     * @param name  计数器名称
     * @param value 累加的值，仅支持：Long、Double、Integer类型
     */
    protected void addMultiCounter(String name, Number value) {
        ValueUtils.requireNonNull(name, "计数器名称不能为空！");
        ValueUtils.requireNonNull(value, "累加值不能为空");

        if (value instanceof Long) {
            this.addToLongCounter(name, value.longValue());
        } else if (value instanceof Double) {
            this.addToDoubleCounter(name, value.doubleValue());
        } else if (value instanceof Integer) {
            this.addToIntCounter(name, value.intValue());
        } else {
            throw new IllegalArgumentException("暂不支持该计数器类型，当前仅支持：Integer、Long、Double类型");
        }
    }

    /**
     * 将累加值添加到指定的Long计数器中
     *
     * @param name  Long计数器名称
     * @param value 累加的值
     * @return Long计数器实例
     */
    private LongCounter addToLongCounter(String name, long value) {
        LongCounter longCounter = this.getLongCounter(name);
        longCounter.add(value);
        return longCounter;
    }

    /**
     * 获取指定的LongCount计数器
     *
     * @param name 计数器名称
     * @return Long计数器
     */
    protected LongCounter getLongCounter(String name) {
        ValueUtils.requireNonNull(name, "LongCounter计数器名称不能为空！");
        return this.getRuntimeContext().getLongCounter(name);
    }

    /**
     * 将累加值添加到指定的Double计数器中
     *
     * @param name  Double计数器名称
     * @param value 累加的值
     * @return Double计数器实例
     */
    private DoubleCounter addToDoubleCounter(String name, double value) {
        DoubleCounter doubleCounter = this.getDoubleCounter(name);
        doubleCounter.add(value);
        return doubleCounter;
    }

    /**
     * 获取指定的DoubleCount计数器
     *
     * @param name 计数器名称
     * @return Double计数器
     */
    protected DoubleCounter getDoubleCounter(String name) {
        ValueUtils.requireNonNull(name, "DoubleCounter计数器名称不能为空！");
        return this.getRuntimeContext().getDoubleCounter(name);
    }

    /**
     * 将累加值添加到指定的Int计数器中
     *
     * @param name  Int计数器名称
     * @param value 累加的值
     * @return Int计数器实例
     */
    private IntCounter addToIntCounter(String name, int value) {
        IntCounter intCounter = null;
        try {
            intCounter = this.getIntCounter(name);
        } catch (Exception e) {
            System.out.println("获取Int计数器失败，尝试注册新的Int计数器");
        }
        intCounter.add(value);
        return intCounter;
    }

    /**
     * 获取指定的IntCount计数器
     *
     * @param name 计数器名称
     * @return int计数器
     */
    protected IntCounter getIntCounter(String name) {
        ValueUtils.requireNonNull(name, "IntCounter计数器名称不能为空！");
        return this.getRuntimeContext().getIntCounter(name);
    }

    /**
     * 注册新的累加器
     *
     * @param name        累加器名称
     * @param accumulator 累加器实例
     */
    protected <V, A extends Serializable> void addAccumulator(String name, Accumulator<V, A> accumulator) {
        ValueUtils.requireNonNull(name, "累加器名称不能为空！");
        ValueUtils.requireNonNull(accumulator, "累加器实例不能为空！");
        this.getRuntimeContext().addAccumulator(name, accumulator);
    }

    /**
     * 根据累加器名称获取累加器对象实例
     *
     * @param name 累加器名称
     * @return 累加器实例
     */
    protected <V, A extends Serializable> Accumulator<V, A> getAccumulator(String name) {
        ValueUtils.requireNonNull(name, "累加器名称不能为空！");
        return this.getRuntimeContext().getAccumulator(name);
    }

    /**
     * 根据文件名获取分布式缓存文件
     *
     * @param fileName 缓存文件名称
     * @return 被缓存的文件
     */
    protected File getCacheFile(String fileName) {
        ValueUtils.requireNonNull(fileName, "分布式缓存文件名称不能为空！");
        return this.getDistributedCache().getFile(fileName);
    }
}
