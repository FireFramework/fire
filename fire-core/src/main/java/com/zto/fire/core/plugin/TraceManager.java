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
import com.zto.fire.core.bean.TraceTarget;
import net.bytebuddy.agent.ByteBuddyAgent;
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
 * Trace代码增强管理器，负责ByteBuddy运行时安装与卸载
 *
 * @author ChengLong
 * @since 3.0.0 2026-05-06 13:01:30
 */
public final class TraceManager {
    private static final Logger logger = Logger.getLogger(TraceManager.class);
    private static final AtomicBoolean started = new AtomicBoolean(false);
    private static volatile ResettableClassFileTransformer resettable;
    // 存放 pattern → 耗时阈值（毫秒）
    private static final ConcurrentHashMap<String, Long> thresholdByPatternMap = new ConcurrentHashMap<>();

    private TraceManager() {
    }

    /**
     * 任务启动时按框架配置开启 Trace：从 {@code fire.trace.codeTrace.class} 等配置解析出 {@link TraceTarget} 列表，
     * 再转调 {@link #startCodeTrace(List)}，与 REST 动态下发共用同一套挂桩逻辑。
     */
    public static void startCodeTrace() {
        // 配置项逗号分隔的「类.方法 / 类.*」→ 多条 TraceTarget（阈值取 fire.trace 配置）
        List<TraceTarget> targetFromConf = buildTargetsFromFireConf();
        if (targetFromConf.isEmpty()) {
            logger.warn("Trace 配置目标为空，请配置 fire.trace.codeTrace.class");
            return;
        }
        startCodeTrace(targetFromConf);
    }

    /**
     * 启动 Trace：将每条 target 转为pattern → 阈值
     *
     * @param targets 需要追踪的类/方法 pattern 及可选阈值
     */
    public static void startCodeTrace(List<TraceTarget> targets) {
        if (targets == null || targets.isEmpty()) {
            logger.warn("Trace targets 为空，忽略启动");
            return;
        }

        // 单条 target 未显式指定阈值时，与配置启动一致，统一用框架配置默认值
        final long defaultThresholdMs = FireFrameworkConf.traceCodeTraceThresholdMs();
        // 过滤空项后保留请求顺序：每条变为 (pattern, 实际阈值) 便于后续拆成 list / map
        List<AbstractMap.SimpleImmutableEntry<String, Long>> ordered = targets.stream()
                .filter(target -> target != null && StringUtils.isNotBlank(target.getPattern()))
                .map(target -> {
                    String pattern = target.getPattern().trim();
                    long thresholdMs = target.getThresholdMs() == null ? defaultThresholdMs : Math.max(target.getThresholdMs(), 0L);
                    return new AbstractMap.SimpleImmutableEntry<>(pattern, thresholdMs);
                })
                .collect(Collectors.toList());

        // 与 thresholdMap 的 key 顺序一致，供 ByteBuddy 匹配器与日志展示使用
        List<String> patternList = ordered.stream()
                .map(AbstractMap.SimpleImmutableEntry::getKey)
                .collect(Collectors.toList());

        // LinkedHashMap：与 pattern 顺序一致，resolveThresholdMs 按 pattern key 查找
        Map<String, Long> targetMap = ordered.stream()
                .collect(Collectors.toMap(
                        e -> buildThresholdKey(e.getKey()),
                        AbstractMap.SimpleImmutableEntry::getValue,
                        (a, b) -> b,
                        LinkedHashMap::new));

        // 日志展示：保留调用方传入的 pattern 顺序
        String displayAgg = String.join(",", patternList);
        installTrace(displayAgg, patternList, targetMap);
    }

    /**
     * 冷启动的方式：从配置中读取目标类列表信息
     */
    private static List<TraceTarget> buildTargetsFromFireConf() {
        String targetStr = FireFrameworkConf.traceCodeTraceClass();
        if (StringUtils.isBlank(targetStr)) {
            return new ArrayList<>();
        }

        // 从配置中读取阈值，没有接口的方式灵活，配置的方式只能设置一个统一的阈值
        final long thresholdMs = FireFrameworkConf.traceCodeTraceThresholdMs();
        return Arrays.stream(targetStr.split(","))
                .map(String::trim)
                .filter(StringUtils::isNotBlank)
                .map(part -> {
                    TraceTarget traceTarget = new TraceTarget();
                    traceTarget.setPattern(part);
                    traceTarget.setThresholdMs(thresholdMs);
                    return traceTarget;
                })
                .collect(Collectors.toList());
    }

    /**
     * 按本次入参全量生效：若当前已挂桩则先 stop，再安装 ByteBuddy 并写入阈值表
     *
     * @param displayAgg   仅用于日志，为本次 pattern 按请求顺序逗号拼接
     * @param patterns     供 ByteBuddy 构造匹配器与织入类型范围
     * @param thresholdMap pattern 规范化 key → 阈值毫秒
     */
    private static void installTrace(String displayAgg, List<String> patterns,
                                     Map<String, Long> thresholdMap) {
        synchronized (TraceManager.class) {
            if (started.get()) {
                stopCodeTrace();
            }

            if (started.compareAndSet(false, true)) {
                try {
                    thresholdByPatternMap.clear();
                    thresholdByPatternMap.putAll(thresholdMap);
                    System.setProperty("jdk.attach.allowAttachSelf", "true");
                    ByteBuddyAgent.install();
                    Instrumentation instrumentation = ByteBuddyAgent.getInstrumentation();
                    ElementMatcher.Junction<MethodDescription> methodsOnly = buildMethodMatcher(patterns);
                    AgentBuilder.Transformer.ForAdvice advice = newTimingAdviceTransformer()
                            .advice(methodsOnly, TraceTimingAdvice.class.getName());

                    String[] typeNames = distinctTargets(patterns);
                    resettable = new AgentBuilder.Default()
                            .disableClassFormatChanges()
                            .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION)
                            .with(AgentBuilder.Listener.StreamWriting.toSystemError().withErrorsOnly())
                            .type(ElementMatchers.namedOneOf(typeNames))
                            .transform(advice)
                            .installOn(instrumentation);
                    logger.warn(String.format("Trace 已启动：targets=%s confThresholdMs=%d", displayAgg, FireFrameworkConf.traceCodeTraceThresholdMs()));
                } catch (Throwable e) {
                    started.compareAndSet(true, false);
                    thresholdByPatternMap.clear();
                    logger.error(String.format("Trace 启动失败：targets=%s", displayAgg), e);
                }
            }
        }
    }

    /**
     * 根据pattern处理，返回合法的key
     */
    private static String buildThresholdKey(String pattern) {
        return pattern == null ? "" : pattern.trim();
    }

    /**
     * 停止字节码增强
     */
    public static void stopCodeTrace() {
        synchronized (TraceManager.class) {
            if (!started.compareAndSet(true, false)) {
                return;
            }

            try {
                if (resettable != null) {
                    Instrumentation instrumentation = ByteBuddyAgent.getInstrumentation();
                    resettable.reset(instrumentation, AgentBuilder.RedefinitionStrategy.RETRANSFORMATION);
                    resettable = null;
                }
                logger.warn("Trace 已停止");
            } catch (Throwable e) {
                logger.error("Trace 停止失败", e);
            } finally {
                thresholdByPatternMap.clear();
            }
        }
    }

    /**
     * 重启代码增强
     */
    public static void restartCodeTrace(List<TraceTarget> targets) {
        stopCodeTrace();
        startCodeTrace(targets);
    }

    /**
     * 打印耗时超过阈值的方法调用日志
     */
    public static void printTraceLog(long start, String declaringType, String methodName,
                                     Object[] allArgs, Object result, Throwable thrown) {
        long cost = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        long needMs = resolveThresholdMs(declaringType, methodName);
        if (cost < needMs) {
            return;
        }

        String origin = declaringType + "." + methodName;
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
     * 根据pattern获取耗时阈值
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
     * 从追踪目标中提取去重后的类名列表
     *
     * @param targets 追踪目标列表
     * @return 去重后的全限定类名数组
     */
    private static String[] distinctTargets(List<String> targets) {
        return targets.stream()
                .map(TraceManager::parseClassName)
                .collect(Collectors.toCollection(LinkedHashSet::new))
                .toArray(new String[0]);
    }

    /**
     * 从单个追踪目标中解析类名
     *
     * @param pattern 单个追踪目标，格式为全限定类名.方法名或全限定类名.*
     * @return 全限定类名
     */
    private static String parseClassName(String pattern) {
        return splitClassAndMethod(pattern)[0];
    }

    /**
     * 将追踪目标拆分为类名与方法名
     *
     * @param pattern 单个追踪目标，格式为全限定类名.方法名或全限定类名.*
     * @return 长度为2的数组，依次为类名、方法名
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
     * 构造单个追踪目标对应的普通方法匹配器
     *
     * @param pattern 单个追踪目标
     * @return ByteBuddy普通方法匹配器
     */
    private static ElementMatcher.Junction<MethodDescription> methodMatcherForTargetMethods(String pattern) {
        String[] classAndMethod = splitClassAndMethod(pattern);
        ElementMatcher.Junction<MethodDescription> onType = ElementMatchers.isMethod()
                .and(ElementMatchers.isDeclaredBy(ElementMatchers.named(classAndMethod[0])));
        return "*".equals(classAndMethod[1]) ? onType : onType.and(ElementMatchers.named(classAndMethod[1]));
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
