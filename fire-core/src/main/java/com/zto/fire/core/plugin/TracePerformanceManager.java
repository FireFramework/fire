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
import com.zto.fire.core.bean.TracePerformanceTarget;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.agent.builder.ResettableClassFileTransformer;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.matcher.ElementMatcher;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.lang.instrument.Instrumentation;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Trace performance代码增强管理器，负责ByteBuddy运行时安装与卸载
 *
 * @author ChengLong
 * @since 3.0.0 2026-05-06 13:01:30
 */
public final class TracePerformanceManager extends TraceManager {
    private static final Logger logger = Logger.getLogger(TracePerformanceManager.class);
    private static final AtomicBoolean started = new AtomicBoolean(false);
    private static volatile ResettableClassFileTransformer resettable;
    // 缓存当前生效的pattern和耗时阈值数据，运行时间超过该阈值的方法才会被打印
    private static final ConcurrentHashMap<String, Long> thresholdByPatternMap = new ConcurrentHashMap<>();

    private TracePerformanceManager() {
    }

    /**
     * 冷启动：基于配置的方式启动代码增强
     */
    public static void startTracePerformance() {
        // 配置项逗号分隔的类.方法或类.*
        List<TracePerformanceTarget> targetFromConf = buildTargetsFromFireConf();
        if (targetFromConf.isEmpty()) {
            logger.warn("Trace performance 配置目标为空，请配置 fire.trace.codeTrace.class");
            return;
        }
        startTracePerformance(targetFromConf);
    }

    /**
     * 启动 Trace performance：将每条 target 转为pattern → 阈值
     *
     * @param targets 需要追踪的类/方法 pattern 及可选阈值
     */
    public static void startTracePerformance(List<TracePerformanceTarget> targets) {
        if (targets == null || targets.isEmpty()) {
            logger.warn("Trace performance targets 为空，忽略启动");
            return;
        }

        final long defaultThresholdMs = FireFrameworkConf.traceCodeTraceThresholdMs();
        // 过滤空配置并补齐阈值，保留 targets 的原始配置顺序，不做排序
        List<AbstractMap.SimpleImmutableEntry<String, Long>> ordered = targets.stream()
                .filter(target -> target != null && StringUtils.isNotBlank(target.getPattern()))
                .map(target -> {
                    String pattern = target.getPattern().trim();
                    long thresholdMs = target.getThresholdMs() == null ? defaultThresholdMs : Math.max(target.getThresholdMs(), 0L);
                    return new AbstractMap.SimpleImmutableEntry<>(pattern, thresholdMs);
                })
                .collect(Collectors.toList());

        // 提取 pattern 列表，供 ByteBuddy 构造类型/方法匹配器，同时用于启动日志展示
        List<String> patternList = ordered.stream()
                .map(AbstractMap.SimpleImmutableEntry::getKey)
                .collect(Collectors.toList());

        // 构造 pattern -> thresholdMs 映射，后续执行时按「类.方法」或「类.*」查询该方法的耗时阈值
        Map<String, Long> targetMap = ordered.stream()
                .collect(Collectors.toMap(
                        e -> buildThresholdKey(e.getKey()),
                        AbstractMap.SimpleImmutableEntry::getValue,
                        (a, b) -> b,
                        LinkedHashMap::new));

        // 对patternList中指定的所有的类和方法进行增强
        installTracePerformance(patternList, targetMap);
    }

    /**
     * 冷启动的方式：从配置中读取目标类列表信息
     */
    private static List<TracePerformanceTarget> buildTargetsFromFireConf() {
        String targetStr = FireFrameworkConf.traceCodeTraceClass();
        if (StringUtils.isBlank(targetStr)) {
            return new ArrayList<>();
        }

        final long thresholdMs = FireFrameworkConf.traceCodeTraceThresholdMs();
        return Arrays.stream(targetStr.split(","))
                .map(String::trim)
                .filter(StringUtils::isNotBlank)
                .map(part -> {
                    TracePerformanceTarget traceTarget = new TracePerformanceTarget();
                    traceTarget.setPattern(part);
                    traceTarget.setThresholdMs(thresholdMs);
                    return traceTarget;
                })
                .collect(Collectors.toList());
    }

    /**
     * 安装本次 Trace performance 配置：若已存在增强则先停止旧配置，再刷新阈值表并安装新的 ByteBuddy transformer
     */
    private static void installTracePerformance(List<String> patterns, Map<String, Long> thresholdMap) {
        synchronized (TracePerformanceManager.class) {
            if (started.get()) {
                stopTracePerformance();
            }

            if (started.compareAndSet(false, true)) {
                try {
                    thresholdByPatternMap.clear();
                    thresholdByPatternMap.putAll(thresholdMap);
                    Instrumentation instrumentation = installByteBuddyAgent();
                    ElementMatcher.Junction<MethodDescription> methodsOnly = buildMethodMatcher(patterns);
                    AgentBuilder.Transformer.ForAdvice advice = newAdviceTransformer(TracePerformanceAdvice.class)
                            .advice(methodsOnly, TracePerformanceAdvice.class.getName());

                    String[] typeNames = distinctTargets(patterns);
                    resettable = newDefaultAgentBuilder()
                            .type(ElementMatchers.namedOneOf(typeNames))
                            .transform(advice)
                            .installOn(instrumentation);
                    String targets = String.join(",", patterns);
                    logger.warn(String.format("Trace performance 已启动：targets=%s confThresholdMs=%d", targets, FireFrameworkConf.traceCodeTraceThresholdMs()));
                } catch (Throwable e) {
                    started.compareAndSet(true, false);
                    thresholdByPatternMap.clear();
                    logger.error(String.format("Trace performance 启动失败"), e);
                }
            }
        }
    }

    private static String buildThresholdKey(String pattern) {
        return pattern == null ? "" : pattern.trim();
    }

    /**
     * 停止字节码增强
     */
    public static void stopTracePerformance() {
        synchronized (TracePerformanceManager.class) {
            if (!started.compareAndSet(true, false)) {
                return;
            }

            try {
                if (resettable != null) {
                    resetTransformer(resettable);
                    resettable = null;
                }
                logger.warn("Trace performance 已停止");
            } catch (Throwable e) {
                logger.error("Trace performance 停止失败", e);
            } finally {
                thresholdByPatternMap.clear();
            }
        }
    }

    /**
     * 重启代码增强
     */
    public static void restartTracePerformance(List<TracePerformanceTarget> targets) {
        stopTracePerformance();
        startTracePerformance(targets);
    }

    /**
     * 打印耗时超过阈值的方法调用日志
     */
    public static void printTracePerformanceLog(long start, String declaringType, String methodName,
                                                Object[] allArgs, Object result, Throwable thrown) {
        long cost = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        long needMs = resolveThresholdMs(declaringType, methodName);
        if (cost < needMs) {
            return;
        }

        String origin = declaringType + "." + methodName;
        StringBuilder builder = new StringBuilder();
        builder.append("[TracePerformance] 方法名称：").append(origin)
                .append(" 参数：").append(allArgs == null ? "[]" : Arrays.deepToString(allArgs))
                .append(" 返回值：").append(result)
                .append(" 耗时：").append(cost).append("ms");

        if (thrown != null) {
            builder.append(" 异常：").append(thrown.getClass().getName()).append(": ").append(thrown.getMessage());
        }
        logger.warn(builder.toString());
    }

    /**
     * 根据pattern获取阈值
     *
     * @param pattern 类名
     * @param methodName 方法名称
     * @return 耗时阈值
     */
    private static long resolveThresholdMs(String pattern, String methodName) {
        String dt = pattern == null ? "" : pattern.trim();
        String mn = methodName == null ? "" : methodName.trim();
        Long threshold = thresholdByPatternMap.get(buildThresholdKey(dt + "." + mn));

        if (threshold != null) {
            return threshold;
        }

        threshold = thresholdByPatternMap.get(buildThresholdKey(dt + ".*"));
        if (threshold != null) {
            return threshold;
        }

        return FireFrameworkConf.traceCodeTraceThresholdMs();
    }

    /**
     * 获取去重后的类名列表
     */
    private static String[] distinctTargets(List<String> targets) {
        return targets.stream()
                .map(TracePerformanceManager::parseClassName)
                .collect(Collectors.toCollection(LinkedHashSet::new))
                .toArray(new String[0]);
    }

    private static String parseClassName(String pattern) {
        return splitClassAndMethod(pattern)[0];
    }

    /**
     * 根据pattern拆分class和方法/通配符
     */
    private static String[] splitClassAndMethod(String pattern) {
        if (StringUtils.isBlank(pattern)) {
            throw new IllegalArgumentException("target不能为空");
        }

        String target = pattern.trim();
        int lastDot = target.lastIndexOf('.');
        if (lastDot <= 0) {
            throw new IllegalArgumentException("必须是：全限定类名.方法名 或 全限定类名.*：" + pattern);
        }

        String cName = target.substring(0, lastDot).trim();
        String methodSpec = target.substring(lastDot + 1).trim();
        if (StringUtils.isBlank(cName) || StringUtils.isBlank(methodSpec)) {
            throw new IllegalArgumentException("类名或方法名不能为空：" + pattern);
        }

        return new String[]{cName, methodSpec};
    }

    private static ElementMatcher.Junction<MethodDescription> buildMethodMatcher(List<String> targets) {
        return targets.stream()
                .map(TracePerformanceManager::methodMatcherForTargetMethods)
                .reduce(ElementMatcher.Junction::or)
                .orElseGet(ElementMatchers::none);
    }

    private static ElementMatcher.Junction<MethodDescription> methodMatcherForTargetMethods(String pattern) {
        String[] classAndMethod = splitClassAndMethod(pattern);
        ElementMatcher.Junction<MethodDescription> onType = ElementMatchers.isMethod()
                .and(ElementMatchers.isDeclaredBy(ElementMatchers.named(classAndMethod[0])));
        return "*".equals(classAndMethod[1]) ? onType : onType.and(ElementMatchers.named(classAndMethod[1]));
    }
}
