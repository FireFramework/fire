package org.apache.rocketmq.flink.serialization;

import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.io.Serializable;

/**
 * 反序列化，携带tag信息
 * @author ChengLong 2021-5-10 09:43:35
 */
public interface TagKeyValueDeserializationSchema<T> extends ResultTypeQueryable<T>, Serializable {

    T deserializeTagKeyAndValue(byte[] tag, byte[] key, byte[] value);
}