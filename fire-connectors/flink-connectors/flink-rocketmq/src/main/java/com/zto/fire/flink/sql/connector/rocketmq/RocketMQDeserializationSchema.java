package com.zto.fire.flink.sql.connector.rocketmq;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;
import org.apache.rocketmq.common.message.MessageExt;

import java.io.Serializable;

public interface RocketMQDeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {

    default void open(DeserializationSchema.InitializationContext context) throws Exception {}

    T deserialize(MessageExt message) throws Exception;

    default void deserialize(MessageExt message, Collector<T> collector) throws Exception {
        T record = deserialize(message);
        if (record != null) collector.collect(record);
    }

    boolean isEndOfStream(T record);
}
