package org.apache.rocketmq.flink.serialization;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.rocketmq.common.message.Message;

import java.io.Serializable;

/**
 * 序列化，携带tag信息
 *
 * @author ChengLong 2021-8-17 13:32:21
 */
public interface TagKeyValueSerializationSchema<T> extends Serializable {

    default void open(SerializationSchema.InitializationContext context) throws Exception {
    }

    Message serialize(T element);
}
