package com.zto.fire.mq.utils;

import com.zto.bigdata.integeration.util.ZtoSerializeUtil;
import com.zto.fire.common.util.ReflectionUtils;
import com.zto.fire.flink.conf.ZtoSerializeConf$;
import com.zto.zdata.schema.format.InputData;

import java.lang.reflect.Field;
import java.util.Collection;

/**
 * 序列化工具类
 */
public class DeserializeUtil {
    /**
     * 初始化序列化器（根据 topic）
     * @param topic 主题名称
     */
    public static void initZtoSerializer(String topic) throws Throwable {
        Class deserializationClass = ReflectionUtils.forName(ZtoSerializeConf$.MODULE$.SERIALIZER_CLASS_NAME());
        if (deserializationClass != null) {
            ReflectionUtils.getMethodByName(deserializationClass, ZtoSerializeConf$.MODULE$.SERIALIZER_CLASS_INIT_METHOD_NAME()).invoke(null, topic, null);
        }
    }

    /**
     * 统一反序列化 + 反射赋值
     *
     * @param <T>        目标对象类型
     * @param clazz     目标 Class
     * @param headType  序列化类型（header 中的值）
     * @param value     原始 byte[]
     * @return T
     */
    public static <T> T deserializeAndFill(Class<T> clazz, String headType, byte[] value) {
        try {
            InputData inputData = ZtoSerializeUtil.serializerManager.deserialize(headType, value);
            T target = clazz.getDeclaredConstructor().newInstance();
            // 3. 反射字段赋值（包含 private）
            Collection<Field> fields = ReflectionUtils.getAllFields(clazz).values();
            for(Field field : fields){
                field.setAccessible(true);
                Object fieldValue = inputData.getObject(field.getName());
                field.set(target, fieldValue);
            }
            return target;
        } catch (Exception e) {
            throw new RuntimeException("反序列化失败，class=" + clazz.getName(), e);
        }
    }
}
