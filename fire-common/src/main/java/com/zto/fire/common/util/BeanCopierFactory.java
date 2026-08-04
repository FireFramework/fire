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

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.dynamic.scaffold.InstrumentedType;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.jar.asm.Opcodes;
import net.bytebuddy.jar.asm.Type;
import net.bytebuddy.matcher.ElementMatchers;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 基于 ByteBuddy 生成 {@link BeanCopier} 实现类的工厂
 * <p>
 * 仅在首次针对某组 source/target 类型时做属性扫描与字节码生成；
 * 生成的拷贝逻辑为直接方法调用，等价于手写 getter/setter 赋值
 * </p>
 * <p>
 * 流程概要：解析属性映射 → ByteBuddy 生成实现 {@link BeanCopier} 的类 → 注入 ClassLoader → new 实例返回
 * </p>
 *
 * @author ChengLong
 * @since 3.0.0
 */
final class BeanCopierFactory {

    // 生成类名后缀序号，避免同名类型对并发生成时类名冲突
    private static final AtomicLong SEQUENCE = new AtomicLong();

    private BeanCopierFactory() {
    }

    /**
     * 为指定 source/target 类型对生成拷贝器实例
     *
     * @param ignoreCaseAndUnderline 为 true 时按忽略大小写与下划线的规则匹配字段
     */
    static BeanCopier create(Class<?> sourceClass, Class<?> targetClass, boolean ignoreCaseAndUnderline) {
        // 1. 用 Introspector 找出可拷贝的属性映射
        List<PropertyMapping> mappings = resolveMappings(sourceClass, targetClass, ignoreCaseAndUnderline);
        String className = buildClassName(sourceClass, targetClass, ignoreCaseAndUnderline);

        try {
            // 2. 生成实现 BeanCopier.copy 的动态类，方法体由 CopyImplementation 生成字节码
            DynamicType.Unloaded<Object> unloaded = new ByteBuddy()
                    .subclass(Object.class)
                    .name(className)
                    .implement(BeanCopier.class)
                    .method(ElementMatchers.named("copy").and(ElementMatchers.takesArguments(2)))
                    .intercept(new CopyImplementation(sourceClass, targetClass, mappings))
                    .make();

            // 3. 注入到可用 ClassLoader，并实例化
            ClassLoader classLoader = resolveClassLoader(sourceClass, targetClass);
            Class<? extends BeanCopier> copierClass = unloaded
                    .load(classLoader, ClassLoadingStrategy.Default.INJECTION)
                    .getLoaded()
                    .asSubclass(BeanCopier.class);

            return copierClass.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to generate BeanCopier for " + sourceClass.getName() + " -> " + targetClass.getName(), e);
        }
    }

    /**
     * 为 {@link NamedValueSource} → target 生成拷贝器：
     * {@code target.setXxx(convert(source.getObject("xxx")))}
     * <p>
     * {@code ignoreCaseAndUnderline} 目前不影响取值名（始终用 target 的 JavaBean 属性名调用
     * {@link NamedValueSource#getObject(String)}），保留参数以便与 Bean→Bean 路径 API 对齐及后续扩展。
     * </p>
     */
    static BeanCopier createNamedValue(Class<?> targetClass, boolean ignoreCaseAndUnderline) {
        List<NamedValueMapping> mappings = resolveNamedValueMappings(targetClass, ignoreCaseAndUnderline);
        String className = buildNamedValueClassName(targetClass, ignoreCaseAndUnderline);

        try {
            DynamicType.Unloaded<Object> unloaded = new ByteBuddy()
                    .subclass(Object.class)
                    .name(className)
                    .implement(BeanCopier.class)
                    .method(ElementMatchers.named("copy").and(ElementMatchers.takesArguments(2)))
                    .intercept(new NamedValueCopyImplementation(targetClass, mappings))
                    .make();

            ClassLoader classLoader = resolveClassLoader(NamedValueSource.class, targetClass);
            Class<? extends BeanCopier> copierClass = unloaded
                    .load(classLoader, ClassLoadingStrategy.Default.INJECTION)
                    .getLoaded()
                    .asSubclass(BeanCopier.class);

            return copierClass.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to generate NamedValue BeanCopier for " + targetClass.getName(), e);
        }
    }

    /**
     * 解析类型兼容的属性映射
     * <p>
     * 条件：source 有 public getter、target 有 public setter、类型兼容；
     * 属性名默认须完全一致；{@code ignoreCaseAndUnderline=true} 时先精确匹配，再按归一化名匹配
     * （去掉下划线并转小写，如 {@code user_name}/{@code USER_NAME}/{@code userName} → {@code username}）
     * </p>
     */
    static List<PropertyMapping> resolveMappings(Class<?> sourceClass, Class<?> targetClass,
                                                 boolean ignoreCaseAndUnderline) {
        Map<String, PropertyDescriptor> sourceProps = ReflectionUtils.beanProperties(sourceClass, true);
        Map<String, PropertyDescriptor> targetProps = ReflectionUtils.beanProperties(targetClass, false);
        List<PropertyMapping> mappings = new ArrayList<>();
        // 已占用的 source / target 属性名，避免同一端被重复映射
        Set<String> mappedSourceNames = new HashSet<>();
        Set<String> mappedTargetNames = new HashSet<>();

        // 第一遍：精确同名匹配（始终优先）
        for (Map.Entry<String, PropertyDescriptor> entry : sourceProps.entrySet()) {
            String name = entry.getKey();
            PropertyDescriptor targetProp = targetProps.get(name);
            if (targetProp == null) {
                continue;
            }
            PropertyMapping mapping = tryCreateMapping(entry.getValue(), targetProp);
            if (mapping != null) {
                mappings.add(mapping);
                mappedSourceNames.add(name);
                mappedTargetNames.add(name);
            }
        }

        if (!ignoreCaseAndUnderline) {
            return mappings;
        }

        // 第二遍：归一化名匹配（忽略大小写与下划线），跳过已精确匹配的两端属性
        Map<String, PropertyDescriptor> targetByNormalized = new HashMap<>();
        for (Map.Entry<String, PropertyDescriptor> entry : targetProps.entrySet()) {
            if (mappedTargetNames.contains(entry.getKey())) {
                continue;
            }
            targetByNormalized.putIfAbsent(normalizePropertyName(entry.getKey()), entry.getValue());
        }

        Set<String> usedNormalizedNames = new HashSet<>();
        for (Map.Entry<String, PropertyDescriptor> entry : sourceProps.entrySet()) {
            String sourceName = entry.getKey();
            if (mappedSourceNames.contains(sourceName)) {
                continue;
            }
            String normalized = normalizePropertyName(sourceName);
            if (!usedNormalizedNames.add(normalized)) {
                continue;
            }
            PropertyDescriptor targetProp = targetByNormalized.get(normalized);
            if (targetProp == null) {
                continue;
            }
            String targetName = targetProp.getName();
            if (mappedTargetNames.contains(targetName)) {
                continue;
            }
            PropertyMapping mapping = tryCreateMapping(entry.getValue(), targetProp);
            if (mapping != null) {
                mappings.add(mapping);
                mappedSourceNames.add(sourceName);
                mappedTargetNames.add(targetName);
            }
        }

        return mappings;
    }

    /**
     * 尝试根据 getter/setter 与类型兼容性创建一条映射，不满足条件时返回 null
     */
    private static PropertyMapping tryCreateMapping(PropertyDescriptor sourceProp, PropertyDescriptor targetProp) {
        Method getter = sourceProp.getReadMethod();
        Method setter = targetProp.getWriteMethod();
        if (getter == null || setter == null) {
            return null;
        }

        // 只拷贝 public 访问器，保证生成的 invokevirtual/invokeinterface 合法
        if (!Modifier.isPublic(getter.getModifiers()) || !Modifier.isPublic(setter.getModifiers())) {
            return null;
        }

        Class<?> fromType = getter.getReturnType();
        Class<?> toType = setter.getParameterTypes()[0];
        if (!ReflectionUtils.isCompatible(fromType, toType)) {
            return null;
        }
        return new PropertyMapping(getter, setter, fromType, toType);
    }

    /**
     * 解析 NamedValue → target 的属性映射：以 target 可写属性为准，
     * 取值名始终为 JavaBean 属性原名（与业务侧 {@code field.getName()} 一致）
     * <p>
     * {@code ignoreCaseAndUnderline} 首版不改变匹配行为，仅保留以对齐 API。
     * </p>
     */
    static List<NamedValueMapping> resolveNamedValueMappings(Class<?> targetClass, boolean ignoreCaseAndUnderline) {
        Map<String, PropertyDescriptor> targetProps = ReflectionUtils.beanProperties(targetClass, false);
        List<NamedValueMapping> mappings = new ArrayList<>(targetProps.size());
        for (Map.Entry<String, PropertyDescriptor> entry : targetProps.entrySet()) {
            PropertyDescriptor descriptor = entry.getValue();
            Method setter = descriptor.getWriteMethod();
            if (setter == null || !Modifier.isPublic(setter.getModifiers())) {
                continue;
            }
            Class<?> toType = setter.getParameterTypes()[0];
            Method getter = descriptor.getReadMethod();
            if (getter != null && !Modifier.isPublic(getter.getModifiers())) {
                getter = null;
            }
            // ignoreCaseAndUnderline 预留：当前始终按属性精确名 getObject
            mappings.add(new NamedValueMapping(entry.getKey(), setter, getter, toType));
        }
        return mappings;
    }

    /**
     * 字段名归一化：去掉下划线并转为小写，用于忽略命名风格差异后的等价判断
     */
    static String normalizePropertyName(String name) {
        if (name == null || name.isEmpty()) {
            return name;
        }
        StringBuilder sb = new StringBuilder(name.length());
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (c != '_') {
                sb.append(Character.toLowerCase(c));
            }
        }
        return sb.toString();
    }

    /**
     * 生成动态类全名，与 {@link BeanCopier} 同包，避免跨包接口可见性问题
     */
    private static String buildClassName(Class<?> sourceClass, Class<?> targetClass, boolean ignoreCaseAndUnderline) {
        return "com.zto.fire.common.util.BeanCopier$"
                + sanitize(sourceClass.getName())
                + "_To_"
                + sanitize(targetClass.getName())
                + (ignoreCaseAndUnderline ? "_Flex_" : "_Exact_")
                + SEQUENCE.incrementAndGet();
    }

    private static String buildNamedValueClassName(Class<?> targetClass, boolean ignoreCaseAndUnderline) {
        return "com.zto.fire.common.util.BeanCopier$NamedValue_To_"
                + sanitize(targetClass.getName())
                + (ignoreCaseAndUnderline ? "_Flex_" : "_Exact_")
                + SEQUENCE.incrementAndGet();
    }

    /**
     * 类名中的特殊字符替换为下划线，保证生成的类名合法
     */
    private static String sanitize(String name) {
        return name.replace('.', '_').replace('$', '_').replace('[', '_').replace(';', '_');
    }

    /**
     * 选择加载动态类的 ClassLoader：
     * 优先 BeanCopier 所在 Loader（保证接口可见），再回退到业务类 / 线程上下文 Loader
     */
    private static ClassLoader resolveClassLoader(Class<?> sourceClass, Class<?> targetClass) {
        ClassLoader classLoader = BeanCopier.class.getClassLoader();
        if (classLoader == null) {
            classLoader = targetClass.getClassLoader();
        }
        if (classLoader == null) {
            classLoader = sourceClass.getClassLoader();
        }
        if (classLoader == null) {
            classLoader = Thread.currentThread().getContextClassLoader();
        }
        return classLoader;
    }

    /**
     * 单条属性映射：source.getter → target.setter，以及两端类型（用于决定是否装箱/拆箱）
     */
    static final class PropertyMapping {
        final Method getter;
        final Method setter;
        final Class<?> fromType;
        final Class<?> toType;

        PropertyMapping(Method getter, Method setter, Class<?> fromType, Class<?> toType) {
            this.getter = getter;
            this.setter = setter;
            this.fromType = fromType;
            this.toType = toType;
        }
    }

    /**
     * NamedValue 单条映射：{@code source.getObject(propertyName)} → {@code target.setter}
     * {@code getter} 可选：primitive 且值为 null 时用 getter 当前值作为回退，实现「跳过覆盖」
     */
    static final class NamedValueMapping {
        final String propertyName;
        final Method setter;
        final Method getter;
        final Class<?> toType;

        NamedValueMapping(String propertyName, Method setter, Method getter, Class<?> toType) {
            this.propertyName = propertyName;
            this.setter = setter;
            this.getter = getter;
            this.toType = toType;
        }
    }

    /**
     * ByteBuddy 的方法实现钩子
     */
    private static final class CopyImplementation implements Implementation {

        private final Class<?> sourceClass;
        private final Class<?> targetClass;
        private final List<PropertyMapping> mappings;

        CopyImplementation(Class<?> sourceClass, Class<?> targetClass, List<PropertyMapping> mappings) {
            this.sourceClass = sourceClass;
            this.targetClass = targetClass;
            this.mappings = mappings;
        }

        /**
         * ByteBuddy 要求实现类提供一个 {@link ByteCodeAppender}，在生成类时由框架回调
         * {@link ByteCodeAppender#apply} 来填充方法体
         * <p>
         * 这里返回的匿名 Appender 只做一件事：把「强转 + 逐属性 get/set」写成 JVM 指令
         * </p>
         */
        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return new ByteCodeAppender() {
                /**
                 * 真正往 {@code copy} 方法里写字节码的入口
                 * <p>
                 * {@code copy(Object source, Object target)} 的局部变量表约定：
                 * <pre>
                 *   slot 0 : this（Copier 实例，本方法不用）
                 *   slot 1 : source（Object）
                 *   slot 2 : target（Object）
                 *   slot 3 : 强转后的 Source 引用（后面反复 ALOAD 3）
                 *   slot 4 : 强转后的 Target 引用（后面反复 ALOAD 4）
                 * </pre>
                 * 操作数栈在每条属性拷贝时大致为：
                 * {@code [targetRef, getter返回值] → invoke setter}
                 * </p>
                 *
                 * @return Size(操作数栈最大深度, 局部变量槽位数)；此处栈深约 5、局部变量用到 slot 4，故为 (5, 5)
                 */
                @Override
                public Size apply(MethodVisitor methodVisitor, Context implementationContext, MethodDescription instrumentedMethod) {
                    // 1. Source s = (Source) source; ----
                    // ALOAD 1：把 slot1 的 source 压栈
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 1);
                    // CHECKCAST：栈顶 Object → Source，失败则 ClassCastException
                    methodVisitor.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(sourceClass));
                    // ASTORE 3：弹出栈顶，存入局部变量 slot3
                    methodVisitor.visitVarInsn(Opcodes.ASTORE, 3);

                    // 2. Target t = (Target) target; ----
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 2);
                    methodVisitor.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(targetClass));
                    methodVisitor.visitVarInsn(Opcodes.ASTORE, 4);

                    // 3. 对每个可拷贝属性生成：t.setXxx( [convert] s.getXxx() ); ----
                    for (PropertyMapping mapping : mappings) {
                        emitCopy(methodVisitor, mapping);
                    }

                    // 4. return;（void）----
                    methodVisitor.visitInsn(Opcodes.RETURN);
                    return new Size(5, 5);
                }
            };
        }

        /**
         * ByteBuddy 生命周期钩子：可在生成前改 InstrumentedType（加字段等）
         * 本实现无额外成员，原样返回即可
         */
        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        /**
         * 为单条属性写出字节码，等价于：
         * {@code t.setter( convert(s.getter()) )}
         * <p>
         * 指令顺序必须配合 JVM 调用约定——实例方法调用时，栈底是接收者，上面是参数：
         * <pre>
         *   ALOAD 4          // 压入 t（setter 的 this）
         *   ALOAD 3          // 压入 s（getter 的 this）
         *   invoke getter    // 弹出 s，压入属性值
         *   [装箱/拆箱]      // 可选：把栈顶值转成 setter 参数类型
         *   invoke setter    // 弹出 (t, 值)，完成赋值
         * </pre>
         * </p>
         */
        private void emitCopy(MethodVisitor mv, PropertyMapping mapping) {
            mv.visitVarInsn(Opcodes.ALOAD, 4);
            mv.visitVarInsn(Opcodes.ALOAD, 3);
            invokeMethod(mv, mapping.getter);

            Class<?> fromType = mapping.fromType;
            Class<?> toType = mapping.toType;
            if (!fromType.equals(toType)) {
                emitConversion(mv, fromType, toType);
            }

            invokeMethod(mv, mapping.setter);
        }

        /**
         * 调用 getter/setter：接口用 INVOKEINTERFACE，类用 INVOKEVIRTUAL
         */
        private void invokeMethod(MethodVisitor mv, Method method) {
            Class<?> declaringClass = method.getDeclaringClass();
            boolean iface = declaringClass.isInterface();
            int opcode = iface ? Opcodes.INVOKEINTERFACE : Opcodes.INVOKEVIRTUAL;
            mv.visitMethodInsn(
                    opcode,
                    Type.getInternalName(declaringClass),
                    method.getName(),
                    Type.getMethodDescriptor(method),
                    iface);
        }

        /**
         * 在栈顶值上生成装箱/拆箱指令（仅基本类型与其包装类型之间）
         * 引用类型的子类→父类赋值无需额外指令
         */
        private void emitConversion(MethodVisitor mv, Class<?> fromType, Class<?> toType) {
            if (fromType.isPrimitive() && !toType.isPrimitive()) {
                // 装箱：如 int → Integer.valueOf(int)
                Class<?> wrapper = ReflectionUtils.wrap(fromType);
                mv.visitMethodInsn(
                        Opcodes.INVOKESTATIC,
                        Type.getInternalName(wrapper),
                        "valueOf",
                        Type.getMethodDescriptor(Type.getType(wrapper), Type.getType(fromType)),
                        false);
                return;
            }

            if (!fromType.isPrimitive() && toType.isPrimitive()) {
                // 拆箱：如 Integer → intValue()
                Class<?> primitive = ReflectionUtils.unwrap(fromType);
                String unboxMethod = ReflectionUtils.unboxMethodName(primitive);
                mv.visitMethodInsn(
                        Opcodes.INVOKEVIRTUAL,
                        Type.getInternalName(fromType),
                        unboxMethod,
                        Type.getMethodDescriptor(Type.getType(primitive)),
                        false);
                return;
            }

            // 引用类型子类→父类：CHECKCAST 通常不需要（setter 参数更宽）
            if (!toType.isAssignableFrom(fromType) && toType.isAssignableFrom(ReflectionUtils.wrap(fromType))) {
                // 已由装箱处理
                return;
            }

            if (!toType.isPrimitive() && !fromType.equals(toType) && toType.isAssignableFrom(fromType)) {
                // 子类赋给父类，无需额外指令
                return;
            }
        }
    }

    /**
     * NamedValueSource → Bean 的 ByteBuddy 实现（无跳转，避免手工 stackmap）：
     * <ul>
     *   <li>引用类型：{@code target.setXxx((T) source.getObject(name))}，允许 null</li>
     *   <li>基本类型：{@code target.setXxx(NamedValueAssigns.toX(v, target.getXxx()))}，null 时保留原值</li>
     * </ul>
     */
    private static final class NamedValueCopyImplementation implements Implementation {

        private final Class<?> targetClass;
        private final List<NamedValueMapping> mappings;

        NamedValueCopyImplementation(Class<?> targetClass, List<NamedValueMapping> mappings) {
            this.targetClass = targetClass;
            this.mappings = mappings;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return new ByteCodeAppender() {
                /**
                 * 局部变量表：
                 * <pre>
                 *   slot 0 : this
                 *   slot 1 : source（Object）
                 *   slot 2 : target（Object）
                 *   slot 3 : NamedValueSource
                 *   slot 4 : Target
                 * </pre>
                 */
                @Override
                public Size apply(MethodVisitor methodVisitor, Context implementationContext,
                                  MethodDescription instrumentedMethod) {
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 1);
                    methodVisitor.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(NamedValueSource.class));
                    methodVisitor.visitVarInsn(Opcodes.ASTORE, 3);

                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 2);
                    methodVisitor.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(targetClass));
                    methodVisitor.visitVarInsn(Opcodes.ASTORE, 4);

                    for (NamedValueMapping mapping : mappings) {
                        emitNamedValueCopy(methodVisitor, mapping);
                    }

                    methodVisitor.visitInsn(Opcodes.RETURN);
                    // 栈深：target + value + fallback(primitive 时 getter 返回值，long/double 占 2 槽) ≈ 4
                    return new Size(4, 5);
                }
            };
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        private void emitNamedValueCopy(MethodVisitor mv, NamedValueMapping mapping) {
            if (mapping.toType.isPrimitive()) {
                emitPrimitiveAssign(mv, mapping);
            } else {
                emitReferenceAssign(mv, mapping);
            }
        }

        /**
         * {@code target.setXxx((T) source.getObject("xxx"));}
         */
        private void emitReferenceAssign(MethodVisitor mv, NamedValueMapping mapping) {
            mv.visitVarInsn(Opcodes.ALOAD, 4);
            mv.visitVarInsn(Opcodes.ALOAD, 3);
            mv.visitLdcInsn(mapping.propertyName);
            mv.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    Type.getInternalName(NamedValueSource.class),
                    "getObject",
                    "(Ljava/lang/String;)Ljava/lang/Object;",
                    true);
            mv.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(mapping.toType));
            invokeMethod(mv, mapping.setter);
        }

        /**
         * {@code target.setXxx(NamedValueAssigns.toInt(source.getObject("xxx"), target.getXxx()));}
         * 无 getter 时 fallback 用类型默认值（0 / false）
         */
        private void emitPrimitiveAssign(MethodVisitor mv, NamedValueMapping mapping) {
            Class<?> primitive = mapping.toType;
            mv.visitVarInsn(Opcodes.ALOAD, 4);

            // arg1: source.getObject(name)
            mv.visitVarInsn(Opcodes.ALOAD, 3);
            mv.visitLdcInsn(mapping.propertyName);
            mv.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    Type.getInternalName(NamedValueSource.class),
                    "getObject",
                    "(Ljava/lang/String;)Ljava/lang/Object;",
                    true);

            // arg2: fallback = getter() or default
            if (mapping.getter != null) {
                mv.visitVarInsn(Opcodes.ALOAD, 4);
                invokeMethod(mv, mapping.getter);
            } else {
                pushPrimitiveDefault(mv, primitive);
            }

            mv.visitMethodInsn(
                    Opcodes.INVOKESTATIC,
                    Type.getInternalName(NamedValueAssigns.class),
                    NamedValueAssigns.methodName(primitive),
                    Type.getMethodDescriptor(Type.getType(primitive), Type.getType(Object.class), Type.getType(primitive)),
                    false);
            invokeMethod(mv, mapping.setter);
        }

        private void pushPrimitiveDefault(MethodVisitor mv, Class<?> primitive) {
            if (primitive == Long.TYPE) {
                mv.visitInsn(Opcodes.LCONST_0);
            } else if (primitive == Double.TYPE) {
                mv.visitInsn(Opcodes.DCONST_0);
            } else if (primitive == Float.TYPE) {
                mv.visitInsn(Opcodes.FCONST_0);
            } else {
                // boolean/byte/short/char/int
                mv.visitInsn(Opcodes.ICONST_0);
            }
        }

        private void invokeMethod(MethodVisitor mv, Method method) {
            Class<?> declaringClass = method.getDeclaringClass();
            boolean iface = declaringClass.isInterface();
            int opcode = iface ? Opcodes.INVOKEINTERFACE : Opcodes.INVOKEVIRTUAL;
            mv.visitMethodInsn(
                    opcode,
                    Type.getInternalName(declaringClass),
                    method.getName(),
                    Type.getMethodDescriptor(method),
                    iface);
        }
    }
}
