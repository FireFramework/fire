package com.zto.fire.common.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer;
import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.esotericsoftware.kryo.serializers.MapSerializer;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Kryo工具类
 * @author ChengLong
 * @date 2017年10月25日11:02:53
 */
public class KryoUtils {

    private static Kryo get() {
        Kryo kryo = new Kryo();
        MapSerializer mapSerializer = new MapSerializer();
        mapSerializer.setKeyClass(String.class, new DefaultSerializers.StringSerializer());
        mapSerializer.setKeysCanBeNull(true);
        mapSerializer.setValueClass(String.class, new DefaultSerializers.StringSerializer());
        mapSerializer.setValuesCanBeNull(true);
        kryo.setDefaultSerializer(CompatibleFieldSerializer.class);
        kryo.setRegistrationRequired(true);
        kryo.register(HashMap.class, mapSerializer);
        kryo.register(Map.class, mapSerializer);
        kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
        return kryo;
    }

    /**
     * 序列化
     *
     * @param object
     * @return
     */
    public static byte[] serializeMap(Map<String, String> object) {
        Output output = new Output(new ByteArrayOutputStream());
        get().writeClassAndObject(output, object);
        byte[] b = output.toBytes();
        output.close();
        return b;
    }

    public static Map<String, String> unserializeMap(byte[] bytes) {
        Input input = new Input(new ByteArrayInputStream(bytes));
        Map<String, String> object = (Map<String, String>) get().readObject(input, HashMap.class);
        input.close();
        return object;
    }

    public static Map<String, String> deserializationMap(byte[] bytes) {
        Map<String, String> map = new HashMap<String, String>();
        try {
            Kryo kryo = new Kryo();
            kryo.setRegistrationRequired(true);
            MapSerializer serializer = new MapSerializer();
            serializer.setKeyClass(String.class, new DefaultSerializers.StringSerializer());
            serializer.setKeysCanBeNull(true);
            serializer.setValueClass(String.class, new DefaultSerializers.StringSerializer());
            serializer.setValuesCanBeNull(true);
            kryo.setDefaultSerializer(CompatibleFieldSerializer.class);
            kryo.register(String.class, new JavaSerializer());
            kryo.register(HashMap.class, serializer, 10);
            Input input = new Input(new ByteArrayInputStream(bytes));
            map = (Map<String, String>) kryo.readClassAndObject(input);
            input.close();
            return map;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
