/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zto.fire.common.util;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 高性能 JavaBean 属性拷贝工具，基于 ByteBuddy 运行时生成专用拷贝类
 * <p>
 * 行为类似 {@code org.apache.commons.beanutils.BeanUtils#copyProperties}，但拷贝阶段不使用反射调用：
 * <ul>
 *   <li>首次：扫描同名（或命名兼容）且类型兼容的属性，生成等价于手写 getter/setter 赋值的 {@link BeanCopier}</li>
 *   <li>之后：从全局缓存取出该 Copier，直接调用（热路径性能接近手写 get/set）</li>
 * </ul>
 * </p>
 * <p>
 * 类型兼容规则：
 * <ul>
 *   <li>类型完全相同</li>
 *   <li>基本类型与对应包装类型互转（如 {@code int} ↔ {@code Integer}）</li>
 *   <li>source 属性类型可赋值给 target 属性类型（含子类 → 父类）</li>
 * </ul>
 * 不做字符串与数字等宽转换
 * </p>
 * <p>
 * 字段名匹配：默认要求属性名完全一致；当 {@code ignoreCaseAndUnderline=true} 时，
 * 忽略大小写与下划线差异（如 {@code user_name}/{@code USER_NAME} 与 {@code userName} 视为同一字段）
 * </p>
 * <p>
 * 另支持 {@link NamedValueSource} 作为源：按 target 属性名调用 {@code getObject(name)} 再写入 public setter。
 * 与 Bean→Bean 路径使用独立缓存键（以 {@link NamedValueSource} 为 source 标记）。
 * </p>
 *
 * @author ChengLong
 * @since 3.0.0
 */
public final class BeanUtils {

    /**
     * 全局 Copier 缓存：key = (sourceClass, targetClass, ignoreCaseAndUnderline)，value = 已生成的拷贝器实例
     * NamedValue 路径的 sourceClass 固定为 {@link NamedValueSource}，与 Bean→Bean 隔离
     * 同一类型对 + 同一匹配策略只生成一次字节码，后续拷贝全部复用
     */
    private static final ConcurrentMap<BeanCopierKey, BeanCopier> COPIER_CACHE = new ConcurrentHashMap<>();

    private BeanUtils() {
    }

    /**
     * 将 source 中与 target 同名且类型兼容的属性拷贝到已有 target 实例
     * <p>
     * 等价于 {@link #copyProperties(Object, Object, boolean) copyProperties(source, target, false)}
     * </p>
     *
     * @param source 源对象，不可为 null
     * @param target 目标对象，不可为 null
     */
    public static void copyProperties(Object source, Object target) {
        copyProperties(source, target, false);
    }

    /**
     * 将 source 中与 target 匹配且类型兼容的属性拷贝到已有 target 实例
     * <p>
     * 热路径：查缓存得到 {@link BeanCopier} → 调用其 {@code copy}（内部为直接 get/set）
     * </p>
     *
     * @param source                  源对象，不可为 null
     * @param target                  目标对象，不可为 null
     * @param ignoreCaseAndUnderline  为 true 时忽略大小写与下划线差异进行字段匹配；为 false 时要求属性名完全一致
     */
    public static void copyProperties(Object source, Object target, boolean ignoreCaseAndUnderline) {
        Objects.requireNonNull(source, "source must not be null");
        Objects.requireNonNull(target, "target must not be null");
        getCopier(source.getClass(), target.getClass(), ignoreCaseAndUnderline).copy(source, target);
    }

    /**
     * 创建 targetClass 的新实例，并将 source 同名兼容属性拷贝后返回
     * <p>
     * 等价于 {@link #copyProperties(Object, Class, boolean) copyProperties(source, targetClass, false)}
     * </p>
     *
     * @param source      源对象，不可为 null
     * @param targetClass 目标类型，不可为 null，且须具备 public 无参构造方法
     * @param <T>         目标类型
     * @return 拷贝后的新实例
     */
    public static <T> T copyProperties(Object source, Class<T> targetClass) {
        return copyProperties(source, targetClass, false);
    }

    /**
     * 创建 targetClass 的新实例，并将 source 匹配的兼容属性拷贝后返回
     * targetClass 必须具备 public 无参构造方法
     *
     * @param source                  源对象，不可为 null
     * @param targetClass             目标类型，不可为 null
     * @param ignoreCaseAndUnderline  为 true 时忽略大小写与下划线差异进行字段匹配；为 false 时要求属性名完全一致
     * @param <T>                     目标类型
     * @return 拷贝后的新实例
     */
    public static <T> T copyProperties(Object source, Class<T> targetClass, boolean ignoreCaseAndUnderline) {
        Objects.requireNonNull(source, "source must not be null");
        Objects.requireNonNull(targetClass, "targetClass must not be null");
        T target = newInstance(targetClass);
        copyProperties(source, target, ignoreCaseAndUnderline);
        return target;
    }

    /**
     * 将 {@link NamedValueSource} 中与 target 属性同名的值拷贝到已有 target 实例
     * <p>
     * 等价于 {@link #copyProperties(NamedValueSource, Object, boolean) copyProperties(source, target, false)}
     * </p>
     *
     * @param source 按名取值源，不可为 null（如 {@code inputData::getObject}）
     * @param target 目标对象，不可为 null
     */
    public static void copyProperties(NamedValueSource source, Object target) {
        copyProperties(source, target, false);
    }

    /**
     * 将 {@link NamedValueSource} 中与 target 属性同名的值拷贝到已有 target 实例
     * <p>
     * 热路径：按 target public setter 生成 {@code source.getObject(propName) → setter} 的专用 Copier。
     * 取值名始终为 target 的 JavaBean 属性名；{@code ignoreCaseAndUnderline} 首版不改变匹配行为，
     * 仅保留以对齐 Bean→Bean API，便于后续扩展候选名。
     * </p>
     * <p>
     * {@code getObject} 返回 null 且 setter 参数为基本类型时跳过赋值（避免拆箱 NPE）。
     * </p>
     *
     * @param source                  按名取值源，不可为 null
     * @param target                  目标对象，不可为 null
     * @param ignoreCaseAndUnderline  预留参数；当前与 {@code false} 行为相同（始终精确属性名 getObject）
     */
    public static void copyProperties(NamedValueSource source, Object target, boolean ignoreCaseAndUnderline) {
        Objects.requireNonNull(source, "source must not be null");
        Objects.requireNonNull(target, "target must not be null");
        getNamedValueCopier(target.getClass(), ignoreCaseAndUnderline).copy(source, target);
    }

    /**
     * 创建 targetClass 的新实例，并从 {@link NamedValueSource} 按属性名赋值后返回
     * <p>
     * 等价于 {@link #copyProperties(NamedValueSource, Class, boolean) copyProperties(source, targetClass, false)}
     * </p>
     *
     * @param source      按名取值源，不可为 null
     * @param targetClass 目标类型，不可为 null，且须具备 public 无参构造方法
     * @param <T>         目标类型
     * @return 拷贝后的新实例
     */
    public static <T> T copyProperties(NamedValueSource source, Class<T> targetClass) {
        return copyProperties(source, targetClass, false);
    }

    /**
     * 创建 targetClass 的新实例，并从 {@link NamedValueSource} 按属性名赋值后返回
     *
     * @param source                  按名取值源，不可为 null
     * @param targetClass             目标类型，不可为 null
     * @param ignoreCaseAndUnderline  预留参数；当前与 {@code false} 行为相同
     * @param <T>                     目标类型
     * @return 拷贝后的新实例
     */
    public static <T> T copyProperties(NamedValueSource source, Class<T> targetClass, boolean ignoreCaseAndUnderline) {
        Objects.requireNonNull(source, "source must not be null");
        Objects.requireNonNull(targetClass, "targetClass must not be null");
        T target = newInstance(targetClass);
        copyProperties(source, target, ignoreCaseAndUnderline);
        return target;
    }

    private static <T> T newInstance(Class<T> targetClass) {
        try {
            // 仅此处使用反射创建目标实例；属性拷贝仍走 ByteBuddy 生成的直接调用
            return targetClass.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Failed to create instance of " + targetClass.getName() + ", public no-arg constructor required", e);
        }
    }

    /**
     * 按类型对与匹配策略获取（或懒创建）对应的 {@link BeanCopier}
     * {@code computeIfAbsent} 保证并发下同一缓存键只生成一次
     */
    private static BeanCopier getCopier(Class<?> sourceClass, Class<?> targetClass, boolean ignoreCaseAndUnderline) {
        BeanCopierKey key = new BeanCopierKey(sourceClass, targetClass, ignoreCaseAndUnderline);
        return COPIER_CACHE.computeIfAbsent(key,
                k -> BeanCopierFactory.create(k.sourceClass, k.targetClass, k.ignoreCaseAndUnderline));
    }

    /**
     * NamedValue 路径专用 Copier：缓存键 source 端固定为 {@link NamedValueSource}
     */
    private static BeanCopier getNamedValueCopier(Class<?> targetClass, boolean ignoreCaseAndUnderline) {
        BeanCopierKey key = new BeanCopierKey(NamedValueSource.class, targetClass, ignoreCaseAndUnderline);
        return COPIER_CACHE.computeIfAbsent(key,
                k -> BeanCopierFactory.createNamedValue(k.targetClass, k.ignoreCaseAndUnderline));
    }

    /**
     * Copier 缓存键：以 Class 对象身份 + 命名匹配策略比较（同一 ClassLoader 下同一 Class 实例唯一）
     */
    private static final class BeanCopierKey {
        private final Class<?> sourceClass;
        private final Class<?> targetClass;
        private final boolean ignoreCaseAndUnderline;

        BeanCopierKey(Class<?> sourceClass, Class<?> targetClass, boolean ignoreCaseAndUnderline) {
            this.sourceClass = sourceClass;
            this.targetClass = targetClass;
            this.ignoreCaseAndUnderline = ignoreCaseAndUnderline;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            BeanCopierKey that = (BeanCopierKey) o;
            // 使用引用相等：Class 对象在同一 ClassLoader 内是单例
            return sourceClass == that.sourceClass
                    && targetClass == that.targetClass
                    && ignoreCaseAndUnderline == that.ignoreCaseAndUnderline;
        }

        @Override
        public int hashCode() {
            int result = 31 * System.identityHashCode(sourceClass) + System.identityHashCode(targetClass);
            return 31 * result + (ignoreCaseAndUnderline ? 1 : 0);
        }
    }
}
