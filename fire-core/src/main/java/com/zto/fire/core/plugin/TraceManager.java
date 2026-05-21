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

package com.zto.fire.core.plugin;

import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.agent.builder.ResettableClassFileTransformer;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.matcher.ElementMatcher;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.commons.lang3.StringUtils;

import java.lang.instrument.Instrumentation;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * ByteBuddy增强管理器基类，统一维护Agent安装、卸载与Advice ClassLoader处理逻辑
 *
 * @author ChengLong
 * @since 3.0.0
 */
public abstract class TraceManager {

    /**
     * 仅允许子类继承，禁止外部直接实例化基类
     */
    protected TraceManager() {
    }

    /**
     * 安装 ByteBuddy Agent 并返回当前 JVM 的 {@link Instrumentation} 实例
     *
     * @return 可用于注册字节码转换器的 Instrumentation
     */
    protected static Instrumentation installByteBuddyAgent() {
        System.setProperty("jdk.attach.allowAttachSelf", "true");
        ByteBuddyAgent.install();
        return ByteBuddyAgent.getInstrumentation();
    }

    /**
     * 获取当前 JVM 的 {@link Instrumentation} 实例（需已执行过 {@link #installByteBuddyAgent()} 或由外部 Agent 提供）
     *
     * @return 当前 JVM 的 Instrumentation
     */
    protected static Instrumentation getInstrumentation() {
        return ByteBuddyAgent.getInstrumentation();
    }

    /**
     * 创建默认 {@link AgentBuilder}，统一重定义策略与错误监听，保证各类 Trace 增强行为一致
     *
     * @return 已配置好的 AgentBuilder 实例
     */
    protected static AgentBuilder newDefaultAgentBuilder() {
        return new AgentBuilder.Default()
                .disableClassFormatChanges()
                .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION)
                .with(AgentBuilder.Listener.StreamWriting.toSystemError().withErrorsOnly());
    }

    /**
     * 创建 Advice 转换器，并将 Advice 类所在 ClassLoader 与当前线程上下文 ClassLoader 纳入可见范围，避免织入时找不到 Advice 类
     *
     * @param adviceClass 被织入的 Advice 类
     * @return 已配置 include ClassLoader 的 ForAdvice 转换器
     */
    protected static AgentBuilder.Transformer.ForAdvice newAdviceTransformer(Class<?> adviceClass) {
        AgentBuilder.Transformer.ForAdvice advice = new AgentBuilder.Transformer.ForAdvice();
        ClassLoader adviceClassLoader = adviceClass.getClassLoader();

        if (adviceClassLoader != null) {
            advice = advice.include(adviceClassLoader);
        }

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        if (contextClassLoader != null) {
            advice = advice.include(contextClassLoader);
        }

        return advice;
    }

    /**
     * 重置（卸载）指定 {@link ResettableClassFileTransformer} 所注册的字节码增强，不影响其他 transformer
     *
     * @param transformer 待重置的转换器，为 null 时不做任何操作
     */
    protected static void resetTransformer(ResettableClassFileTransformer transformer) {
        if (transformer != null) {
            transformer.reset(getInstrumentation(), AgentBuilder.RedefinitionStrategy.RETRANSFORMATION);
        }
    }

    /**
     * 解析 trace pattern：{@code 全限定类名.方法名} 或 {@code 全限定类名.*}
     *
     * @param pattern 类与方法描述串
     * @return 长度为 2 的数组：{@code [0]} 为全限定类名，{@code [1]} 为方法名或字面量 {@code *}
     * @throws IllegalArgumentException pattern 为空、缺少点号分隔或类名/方法名为空
     */
    protected static String[] splitClassAndMethod(String pattern) {
        if (StringUtils.isBlank(pattern)) {
            throw new IllegalArgumentException("pattern不能为空");
        }

        String target = pattern.trim();
        int lastDot = target.lastIndexOf('.');
        if (lastDot <= 0) {
            throw new IllegalArgumentException("必须是：全限定类名.方法名 或 全限定类名.*：" + pattern);
        }

        String className = target.substring(0, lastDot).trim();
        String methodSpec = target.substring(lastDot + 1).trim();
        if (StringUtils.isBlank(className) || StringUtils.isBlank(methodSpec)) {
            throw new IllegalArgumentException("类名或方法名不能为空：" + pattern);
        }

        return new String[]{className, methodSpec};
    }

    /**
     * 从 pattern 中提取全限定类名（最后一个 {@code .} 之前部分）
     *
     * @param pattern {@link #splitClassAndMethod(String)} 可接受的 pattern
     * @return 全限定类名
     */
    protected static String parseClassName(String pattern) {
        return splitClassAndMethod(pattern)[0];
    }

    /**
     * 从多个 pattern 中收集去重后的全限定类名数组，顺序与 patterns 中首次出现的类名一致
     *
     * @param patterns pattern 列表，通常来自配置或 REST 入参
     * @return 去重后的类名数组，可能为空数组
     */
    protected static String[] distinctClassNames(List<String> patterns) {
        return patterns.stream()
                .map(TraceManager::parseClassName)
                .collect(Collectors.toCollection(LinkedHashSet::new))
                .toArray(new String[0]);
    }

    /**
     * 将多个 pattern 合并为单个 ByteBuddy 方法匹配器（各 pattern 之间为「或」关系）
     *
     * @param patterns pattern 列表，元素格式同 {@link #splitClassAndMethod(String)}
     * @return 匹配任一 pattern 的方法描述匹配器；列表为空时匹配 {@code none()}
     */
    protected static ElementMatcher.Junction<MethodDescription> buildMethodMatcher(List<String> patterns) {
        return patterns.stream()
                .map(TraceManager::methodMatcherForPattern)
                .reduce(ElementMatcher.Junction::or)
                .orElseGet(ElementMatchers::none);
    }

    /**
     * 为单个 pattern 构造「声明于指定类型」上的方法匹配器；方法名为 {@code *} 时表示该类型上所有方法
     *
     * @param pattern {@code 类.方法} 或 {@code 类.*}
     * @return 对应 ByteBuddy 方法匹配条件
     */
    private static ElementMatcher.Junction<MethodDescription> methodMatcherForPattern(String pattern) {
        String[] classAndMethod = splitClassAndMethod(pattern);
        ElementMatcher.Junction<MethodDescription> onType = ElementMatchers.isMethod()
                .and(ElementMatchers.isDeclaredBy(ElementMatchers.named(classAndMethod[0])));
        return "*".equals(classAndMethod[1]) ? onType : onType.and(ElementMatchers.named(classAndMethod[1]));
    }

    /**
     * 按类名加载 {@link Class}，不触发静态初始化；优先线程上下文 ClassLoader，失败则回退到 {@link TraceManager} 的加载器
     *
     * @param className 全限定类名
     * @return 加载成功返回 Class，任一步失败返回 null
     */
    protected static Class<?> loadClass(String className) {
        try {
            ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            return Class.forName(className, false, contextClassLoader);
        } catch (Throwable ignored) {
            try {
                return Class.forName(className, false, TraceManager.class.getClassLoader());
            } catch (Throwable ignoredAgain) {
                return null;
            }
        }
    }
}
