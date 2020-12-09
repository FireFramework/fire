package com.zto.fire.common.util;

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 反射工具类，获取各元素信息后缓存到map中
 * Created by ChengLong on 2017-03-30.
 */
public class ReflectionUtils {
    private static final Map<Class<?>, Map<String, Field>> cacheFieldMap = new ConcurrentHashMap<>();
    private static final Map<Class<?>, Map<String, Method>> cacheMethodMap = new ConcurrentHashMap<>();

    private ReflectionUtils() {
    }

    /**
     * 获取所有公有字段，并返回Map
     */
    private static Map<String, Field> getFields(Class<?> clazz) {
        if (clazz == null) {
            return Collections.emptyMap();
        }
        Field[] fields = clazz.getFields();
        if (ValueUtils.isEmpty(fields)) {
            return Collections.emptyMap();
        }
        Map<String, Field> fieldMap = new HashMap<>(fields.length);
        for (Field field : fields) {
            fieldMap.put(field.getName(), field);
        }
        return fieldMap;
    }

    /**
     * 获取所有声明字段，并返回Map
     */
    private static Map<String, Field> getDeclaredFields(Class<?> clazz) {
        if (clazz == null) {
            return Collections.emptyMap();
        }
        Field[] fields = clazz.getDeclaredFields();
        if (ValueUtils.isEmpty(fields)) {
            return Collections.emptyMap();
        }
        Map<String, Field> fieldMap = new HashMap<>(fields.length);
        for (Field field : fields) {
            field.setAccessible(true);
            fieldMap.put(field.getName(), field);
        }
        return fieldMap;
    }

    /**
     * 获取所有字段，含私有和继承而来的，并返回Map
     */
    public static Map<String, Field> getAllFields(Class<?> clazz) {
        if (!cacheFieldMap.containsKey(clazz)) {
            Map<String, Field> fieldMap = new HashMap<>();
            fieldMap.putAll(getFields(clazz));
            fieldMap.putAll(getDeclaredFields(clazz));
            cacheFieldMap.put(clazz, fieldMap);
        }

        return cacheFieldMap.get(clazz);
    }

    /**
     * 根据成员变量名称获取Filed类型（从缓存中获取）
     */
    public static Field getFieldByName(Class<?> clazz, String fieldName) {
        return getAllFields(clazz).get(fieldName);
    }

    /**
     * 获取所有方法，含私有和继承而来的，并返回Map
     */
    public static Map<String, Method> getAllMethods(Class<?> clazz) {
        if (!cacheMethodMap.containsKey(clazz)) {
            Map<String, Method> methodMap = new HashMap<>();
            methodMap.putAll(getMethods(clazz));
            methodMap.putAll(getDeclaredMethods(clazz));
            cacheMethodMap.put(clazz, methodMap);
        }

        return cacheMethodMap.get(clazz);
    }

    /**
     * 根据方法名称获取Method类型（从缓存中获取）
     *
     * @param clazz      类类型
     * @param methodName 方法名称
     * @return Method
     */
    public static Method getMethodByName(Class<?> clazz, String methodName) {
        return getAllMethods(clazz).get(methodName);
    }

    /**
     * 获取所有公有方法，并返回Map
     */
    private static Map<String, Method> getMethods(Class<?> clazz) {
        if (clazz == null) {
            return Collections.emptyMap();
        }
        Method[] methods = clazz.getMethods();
        if (ValueUtils.isEmpty(methods)) {
            return Collections.emptyMap();
        }
        Map<String, Method> methodMap = new HashMap<>(methods.length);
        for (Method method : methods) {
            methodMap.put(method.getName(), method);
        }
        return methodMap;
    }

    /**
     * 获取所有声明方法，并返回Map
     */
    private static Map<String, Method> getDeclaredMethods(Class<?> clazz) {
        if (clazz == null) {
            return Collections.emptyMap();
        }
        Method[] methods = clazz.getDeclaredMethods();
        if (ValueUtils.isEmpty(methods)) {
            return Collections.emptyMap();
        }
        Map<String, Method> methodMap = new HashMap<>(methods.length);
        for (Method method : methods) {
            method.setAccessible(true);
            methodMap.put(method.getName(), method);
        }
        return methodMap;
    }

    /**
     * 获取指定field的类型
     */
    public static Class<?> getFieldType(Class<?> clazz, String fieldName) {
        if (ValueUtils.isExistsEmpty(new Object[]{clazz, fieldName})) {
            return null;
        }
        try {
            Map<String, Field> fieldMap = getAllFields(clazz);
            if (fieldMap == null) {
                return null;
            }
            Field field = fieldMap.get(fieldName);
            if (field != null) {
                return field.getType();
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("指定的Field:" + fieldName + "不存在，请检查");
        }
        return null;
    }

    /**
     * 获取指定的annotation
     *
     * @param scope      annotation所在的位置
     * @param memberName 成员名称，指定获取指定成员的Annotation实例
     */
    private static <T extends Annotation> Annotation getAnnotation(Class<?> clazz, ElementType scope, String memberName, Class<T> annoClass) {
        if (ValueUtils.isExistsEmpty(new Object[]{clazz, scope, memberName, annoClass})) {
            return null;
        }
        try {
            if (ElementType.FIELD == scope) {
                Field field = clazz.getDeclaredField(memberName);
                field.setAccessible(true);
                return field.getAnnotation(annoClass);
            } else if (ElementType.METHOD == scope) {
                Method method = clazz.getDeclaredMethod(memberName);
                method.setAccessible(true);
                return method.getAnnotation(annoClass);
            } else if (ElementType.TYPE == scope) {
                return clazz.getAnnotation(annoClass);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 获取指定的annotation
     *
     * @param scope      annotation所在的位置
     * @param memberName 成员名称，指定获取指定成员的Annotation实例
     */
    private static Annotation[] getAnnotations(Class<?> clazz, ElementType scope, String memberName) {
        if (ValueUtils.isExistsEmpty(new Object[]{clazz, scope, memberName})) {
            return new Annotation[0];
        }
        try {
            if (ElementType.FIELD == scope) {
                Field field = clazz.getDeclaredField(memberName);
                field.setAccessible(true);
                return field.getDeclaredAnnotations();
            } else if (ElementType.METHOD == scope) {
                Method method = clazz.getDeclaredMethod(memberName);
                method.setAccessible(true);
                return method.getDeclaredAnnotations();
            } else if (ElementType.TYPE == scope) {
                return clazz.getDeclaredAnnotations();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new Annotation[0];
    }

    /**
     * 获取Field指定的annotation
     */
    public static <T extends Annotation> Annotation getFieldAnnotation(Class<?> clazz, String fieldName, Class<T> annoClass) {
        return getAnnotation(clazz, ElementType.FIELD, fieldName, annoClass);
    }

    /**
     * 获取Field所有annotation
     */
    public static Annotation[] getFieldAnnotations(Class<?> clazz, String fieldName) {
        return getAnnotations(clazz, ElementType.FIELD, fieldName);
    }

    /**
     * 获取Method指定的annotation
     */
    public static <T extends Annotation> Annotation getMethodAnnotation(Class<?> clazz, String methodName, Class<T> annoClass) {
        return getAnnotation(clazz, ElementType.METHOD, methodName, annoClass);
    }

    /**
     * 获取Method所有annotation
     */
    public static Annotation[] getMethodAnnotations(Class<?> clazz, String methodName) {
        return getAnnotations(clazz, ElementType.METHOD, methodName);
    }

    /**
     * 获取类指定annotation
     */
    public static <T extends Annotation> Annotation getClassAnnotation(Class<?> clazz, Class<T> annoClass) {
        return getAnnotation(clazz, ElementType.TYPE, clazz.getName(), annoClass);
    }

    /**
     * 获取类所有annotation
     */
    public static Annotation[] getClassAnnotations(Class<?> clazz) {
        return getAnnotations(clazz, ElementType.TYPE, clazz.getName());
    }

    /**
     * 获取方法所有参数的所有annotation
     */
    public static Annotation[][] getParamAnnotations(Class<?> clazz, String methodName, Class<?>... parameterTypes) {
        if (ValueUtils.isExistsEmpty(new Object[]{clazz, methodName})) {
            return new Annotation[0][0];
        }
        try {
            Method method = clazz.getDeclaredMethod(methodName, parameterTypes);
            method.setAccessible(true);
            return method.getParameterAnnotations();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new Annotation[0][0];
    }
}
