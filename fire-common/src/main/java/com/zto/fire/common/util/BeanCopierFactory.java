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
import java.util.List;
import java.util.Map;
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
     */
    static BeanCopier create(Class<?> sourceClass, Class<?> targetClass) {
        // 1. 用 Introspector 找出可拷贝的同名属性
        List<PropertyMapping> mappings = resolveMappings(sourceClass, targetClass);
        String className = buildClassName(sourceClass, targetClass);

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
     * 解析同名且类型兼容的属性映射
     * 条件：source 有 public getter、target 有 public setter、属性名相同、类型兼容
     */
    static List<PropertyMapping> resolveMappings(Class<?> sourceClass, Class<?> targetClass) {
        Map<String, PropertyDescriptor> sourceProps = ReflectionUtils.beanProperties(sourceClass, true);
        Map<String, PropertyDescriptor> targetProps = ReflectionUtils.beanProperties(targetClass, false);
        List<PropertyMapping> mappings = new ArrayList<>();

        for (Map.Entry<String, PropertyDescriptor> entry : sourceProps.entrySet()) {
            String name = entry.getKey();
            PropertyDescriptor targetProp = targetProps.get(name);
            if (targetProp == null) {
                continue;
            }

            Method getter = entry.getValue().getReadMethod();
            Method setter = targetProp.getWriteMethod();
            if (getter == null || setter == null) {
                continue;
            }

            // 只拷贝 public 访问器，保证生成的 invokevirtual/invokeinterface 合法
            if (!Modifier.isPublic(getter.getModifiers()) || !Modifier.isPublic(setter.getModifiers())) {
                continue;
            }

            Class<?> fromType = getter.getReturnType();
            Class<?> toType = setter.getParameterTypes()[0];
            if (ReflectionUtils.isCompatible(fromType, toType)) {
                mappings.add(new PropertyMapping(getter, setter, fromType, toType));
            }
        }

        return mappings;
    }

    /**
     * 生成动态类全名，与 {@link BeanCopier} 同包，避免跨包接口可见性问题
     */
    private static String buildClassName(Class<?> sourceClass, Class<?> targetClass) {
        return "com.zto.fire.common.util.BeanCopier$"
                + sanitize(sourceClass.getName())
                + "_To_"
                + sanitize(targetClass.getName())
                + "_"
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
}
