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

import com.zto.fire.common.conf.FireFrameworkConf;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.agent.builder.ResettableClassFileTransformer;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.matcher.ElementMatcher;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.lang.instrument.Instrumentation;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Trace代码增强管理器，负责ByteBuddy运行时安装与卸载
 *
 * @author ChengLong
 * @since 3.0.0
 */
public final class TraceManager {
    private static final Logger logger = Logger.getLogger(TraceManager.class);
    private static final AtomicBoolean started = new AtomicBoolean(false);
    private static final AtomicBoolean stopped = new AtomicBoolean(true);
    private static volatile ResettableClassFileTransformer resettable;
    private static volatile long elapse = 10L;
    private static volatile String className = "";

    /**
     * 工具类禁止实例化
     */
    private TraceManager() {
    }

    /**
     * 启动代码增强
     *
     * @param resourceId     任务资源标识，例如 Spark driver/executor、Flink JobManager 或 container_xxx
     * @param startContainer 为 true 时允许在含 container 的 resourceId 对应进程上启 Trace
     */
    public static void startCodeTrace(String resourceId, boolean startContainer) {
        if (StringUtils.isBlank(resourceId)) {
            throw new IllegalArgumentException("resourceId不能为空，Trace所监控的程序必须有标识！");
        }
        if (resourceId.contains("container") && !startContainer) {
            return;
        }
        startCodeTrace();
    }

    /**
     * 根据配置启动代码增强
     */
    public static void startCodeTrace() {
        startCodeTrace(FireFrameworkConf.traceCodeTraceClass(), FireFrameworkConf.traceCodeTraceElapse());
    }

    /**
     * 启动代码增强
     */
    public static void startCodeTrace(String className, Long elapse) {
        final String targetClass = StringUtils.defaultIfBlank(className, FireFrameworkConf.traceCodeTraceClass());
        final long targetElapse = elapse == null ? FireFrameworkConf.traceCodeTraceElapse() : Math.max(elapse, 0L);
        if (StringUtils.isBlank(targetClass)) {
            logger.warn("Trace代码追踪目标类为空，请通过fire.trace.codeTrace.class进行配置");
            return;
        }

        if (started.compareAndSet(false, true)) {
            try {
                stopped.compareAndSet(true, false);
                TraceManager.className = targetClass;
                TraceManager.elapse = targetElapse;
                System.setProperty("jdk.attach.allowAttachSelf", "true");

                ByteBuddyAgent.install();
                Instrumentation instrumentation = ByteBuddyAgent.getInstrumentation();
                List<String> targets = parseTargets(targetClass);
                String[] typeNames = distinctClassNames(targets);
                ElementMatcher.Junction<MethodDescription> methodsOnly = buildMethodMatcher(targets);
                ElementMatcher.Junction<MethodDescription> constructorsOnly = buildConstructorMatcher(targets);
                AgentBuilder.Transformer.ForAdvice advice = newTimingAdviceTransformer()
                        .advice(methodsOnly, TraceTimingAdvice.class.getName())
                        .advice(constructorsOnly, TraceConstructorTimingAdvice.class.getName());

                resettable = new AgentBuilder.Default()
                        .disableClassFormatChanges()
                        .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION)
                        .with(AgentBuilder.Listener.StreamWriting.toSystemError().withErrorsOnly())
                        .type(ElementMatchers.namedOneOf(typeNames))
                        .transform(advice)
                        .installOn(instrumentation);
                logger.warn(String.format("Trace代码增强服务已启动：className=%s elapse=%sms", targetClass, targetElapse));
            } catch (Throwable e) {
                started.compareAndSet(true, false);
                stopped.compareAndSet(false, true);
                logger.error(String.format("Trace代码增强服务启动失败：className=%s elapse=%sms", targetClass, targetElapse), e);
            }
        } else {
            if (!targetClass.equals(TraceManager.className)) {
                logger.warn(String.format("Trace代码增强目标发生变化，自动重启：oldClassName=%s newClassName=%s", TraceManager.className, targetClass));
                restartCodeTrace(targetClass, targetElapse);
            } else {
                TraceManager.elapse = targetElapse;
                logger.warn(String.format("Trace代码增强服务已处于启动状态，仅更新耗时阈值：className=%s elapse=%sms", targetClass, targetElapse));
            }
        }
    }

    /**
     * 停止代码增强
     */
    public static void stopCodeTrace() {
        if (stopped.compareAndSet(false, true)) {
            try {
                if (resettable != null) {
                    Instrumentation instrumentation = ByteBuddyAgent.getInstrumentation();
                    resettable.reset(instrumentation, AgentBuilder.RedefinitionStrategy.RETRANSFORMATION);
                    resettable = null;
                }
                logger.warn("Trace代码增强服务已停止");
            } catch (Throwable e) {
                logger.error("Trace代码增强服务停止失败", e);
            } finally {
                started.compareAndSet(true, false);
            }
        }
    }

    /**
     * 重启代码增强
     */
    public static void restartCodeTrace(String className, Long elapse) {
        stopCodeTrace();
        startCodeTrace(className, elapse);
    }

    /**
     * 打印耗时超过阈值的方法调用日志
     */
    public static void printTraceLog(long start, String origin, Object[] allArgs, Object result, Throwable thrown) {
        long cost = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        if (cost < elapse) {
            return;
        }

        StringBuilder builder = new StringBuilder();
        builder.append("[TraceCode] 方法名称：").append(origin)
                .append(" 参数：").append(allArgs == null ? "[]" : Arrays.deepToString(allArgs))
                .append(" 返回值：").append(result)
                .append(" 耗时：").append(cost).append("ms");
        if (thrown != null) {
            builder.append(" 异常：").append(thrown.getClass().getName()).append(": ").append(thrown.getMessage());
        }
        logger.warn(builder.toString());
    }

    /**
     * 将配置中的多个追踪目标拆分成列表
     *
     * @param targets 追踪目标配置，多个以逗号分隔
     * @return 追踪目标列表
     */
    private static List<String> parseTargets(String targets) {
        return Arrays.stream(targets.split(","))
                .map(String::trim)
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toList());
    }

    /**
     * 从追踪目标中提取去重后的类名列表
     *
     * @param targets 追踪目标列表
     * @return 去重后的全限定类名数组
     */
    private static String[] distinctClassNames(List<String> targets) {
        return targets.stream()
                .map(TraceManager::parseClassName)
                .collect(Collectors.toCollection(LinkedHashSet::new))
                .toArray(new String[0]);
    }

    /**
     * 从单个追踪目标中解析类名
     *
     * @param raw 单个追踪目标，格式为全限定类名.方法名或全限定类名.*
     * @return 全限定类名
     */
    private static String parseClassName(String raw) {
        return splitClassAndMethod(raw)[0];
    }

    /**
     * 将追踪目标拆分为类名与方法名
     *
     * @param raw 单个追踪目标，格式为全限定类名.方法名或全限定类名.*
     * @return 长度为2的数组，依次为类名、方法名
     */
    private static String[] splitClassAndMethod(String raw) {
        if (StringUtils.isBlank(raw)) {
            throw new IllegalArgumentException("target不能为空");
        }
        String target = raw.trim();
        int lastDot = target.lastIndexOf('.');
        if (lastDot <= 0) {
            throw new IllegalArgumentException("必须是：全限定类名.方法名 或 全限定类名.*，例如 com.zto.fire.Student.print：" + raw);
        }
        String className = target.substring(0, lastDot).trim();
        String methodSpec = target.substring(lastDot + 1).trim();
        if (StringUtils.isBlank(className) || StringUtils.isBlank(methodSpec)) {
            throw new IllegalArgumentException("类名或方法名不能为空：" + raw);
        }
        return new String[]{className, methodSpec};
    }

    /**
     * 根据追踪目标构造普通方法匹配器
     *
     * @param targets 追踪目标列表
     * @return ByteBuddy普通方法匹配器
     */
    private static ElementMatcher.Junction<MethodDescription> buildMethodMatcher(List<String> targets) {
        return targets.stream()
                .map(TraceManager::methodMatcherForTargetMethods)
                .reduce(ElementMatcher.Junction::or)
                .orElseGet(ElementMatchers::none);
    }

    /**
     * 根据追踪目标构造构造器匹配器
     *
     * @param targets 追踪目标列表
     * @return ByteBuddy构造器匹配器
     */
    private static ElementMatcher.Junction<MethodDescription> buildConstructorMatcher(List<String> targets) {
        return targets.stream()
                .map(TraceManager::methodMatcherForTargetConstructors)
                .reduce(ElementMatcher.Junction::or)
                .orElseGet(ElementMatchers::none);
    }

    /**
     * 构造单个追踪目标对应的普通方法匹配器
     *
     * @param raw 单个追踪目标
     * @return ByteBuddy普通方法匹配器
     */
    private static ElementMatcher.Junction<MethodDescription> methodMatcherForTargetMethods(String raw) {
        String[] classAndMethod = splitClassAndMethod(raw);
        ElementMatcher.Junction<MethodDescription> onType = ElementMatchers.isMethod()
                .and(ElementMatchers.isDeclaredBy(ElementMatchers.named(classAndMethod[0])));
        return "*".equals(classAndMethod[1]) ? onType : onType.and(ElementMatchers.named(classAndMethod[1]));
    }

    /**
     * 构造单个追踪目标对应的构造器匹配器
     *
     * @param raw 单个追踪目标
     * @return ByteBuddy构造器匹配器
     */
    private static ElementMatcher.Junction<MethodDescription> methodMatcherForTargetConstructors(String raw) {
        String[] classAndMethod = splitClassAndMethod(raw);
        ElementMatcher.Junction<MethodDescription> onType = ElementMatchers.isConstructor()
                .and(ElementMatchers.isDeclaredBy(ElementMatchers.named(classAndMethod[0])));
        return "*".equals(classAndMethod[1]) ? onType : ElementMatchers.none();
    }

    /**
     * 创建Trace耗时Advice转换器，并加入可定位Advice类的ClassLoader
     *
     * @return ByteBuddy Advice转换器
     */
    private static AgentBuilder.Transformer.ForAdvice newTimingAdviceTransformer() {
        AgentBuilder.Transformer.ForAdvice advice = new AgentBuilder.Transformer.ForAdvice();
        ClassLoader adviceClassLoader = TraceTimingAdvice.class.getClassLoader();
        if (adviceClassLoader != null) {
            advice = advice.include(adviceClassLoader);
        }
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        if (contextClassLoader != null) {
            advice = advice.include(contextClassLoader);
        }
        return advice;
    }
}
