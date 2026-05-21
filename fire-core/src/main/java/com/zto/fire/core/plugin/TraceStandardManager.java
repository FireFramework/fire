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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.zto.fire.common.conf.FireFrameworkConf;
import com.zto.fire.common.util.JSONUtils;
import com.zto.fire.core.bean.TraceStandardApi;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.agent.builder.ResettableClassFileTransformer;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.matcher.ElementMatcher;
import net.bytebuddy.matcher.ElementMatchers;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.lang.instrument.Instrumentation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Trace standard代码规范检测管理器，负责ByteBuddy运行时安装与快速退出
 *
 * @author ChengLong
 * @since 3.0.0
 */
public final class TraceStandardManager extends TraceManager {
    private static final Logger logger = Logger.getLogger(TraceStandardManager.class);
    private static volatile long endNanos = Long.MAX_VALUE;
    private static volatile ResettableClassFileTransformer resettable;
    private static final AtomicBoolean started = new AtomicBoolean(false);
    private static final ConcurrentHashMap<String, TraceStandardApi> apiMappingMap = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Boolean> shouldAnalyzeMap = new ConcurrentHashMap<>();
    private static volatile Map<String, TraceStandardApi> sourcePatternMap = Collections.emptyMap();

    /**
     * 工具类不允许实例化
     */
    private TraceStandardManager() {
    }

    /**
     * 任务启动时按框架配置开启 Trace standard：从fire.trace.codeStandard.api配置中解析原生API与Fire封装映射
     */
    public static void startTraceStandard() {
        // 配置关闭时不安装 ByteBuddy transformer，避免无意义的增强开销
        if (!FireFrameworkConf.traceCodeStandardEnable()) {
            logger.info("Trace standard 未启用，忽略启动");
            return;
        }

        // 从配置中解析「类.方法 -> Fire封装包」映射，作为后续方法匹配与栈过滤依据
        List<TraceStandardApi> mappings = buildApiMappingsFromFireConf();
        if (mappings.isEmpty()) {
            logger.warn("Trace standard api 配置为空，请配置 fire.trace.codeStandard.api");
            return;
        }

        // 启用不规范api使用的检查服务
        startTraceStandard(mappings);
    }

    /**
     * 安装 Trace standard 检测：根据配置的「类.方法」或「类.*」织入目标方法，并在方法入口执行栈分析
     *
     * @param mappings 原生API与Fire封装包的映射列表
     */
    private static void startTraceStandard(List<TraceStandardApi> mappings) {
        synchronized (TraceStandardManager.class) {
            // 该功能仅支持启动时安装一次，退出分析后只在Advice中快速return，不卸载transformer
            if (resettable != null) {
                logger.warn("Trace standard 已启动，忽略重复启动");
                return;
            }

            if (started.compareAndSet(false, true)) {
                try {
                    // 缓存本次配置，并清空运行时解析缓存，避免旧配置影响新一轮安装
                    sourcePatternMap = buildSourcePatternMap(mappings);
                    if (sourcePatternMap.isEmpty()) {
                        started.compareAndSet(true, false);
                        logger.warn("Trace standard api 配置无效，请检查 fire.trace.codeStandard.api 是否为「全限定类名.方法名」或「全限定类名.*」");
                        return;
                    }
                    apiMappingMap.clear();
                    shouldAnalyzeMap.clear();
                    long durationMin = Math.max(FireFrameworkConf.traceCodeStandardDurationMin(), 0L);
                    // 记录分析截止时间，超过 durationMin 后 Advice 将快速返回
                    endNanos = System.nanoTime() + TimeUnit.MINUTES.toNanos(durationMin);

                    List<String> sourcePatterns = new ArrayList<>(sourcePatternMap.keySet());
                    Instrumentation instrumentation = installByteBuddyAgent();
                    // 方法匹配：按配置的「类.方法」或「类.*」精确织入目标方法
                    ElementMatcher.Junction<MethodDescription> methodMatcher = buildMethodMatcher(sourcePatterns)
                            .and(ElementMatchers.not(ElementMatchers.isAbstract()))
                            .and(ElementMatchers.not(ElementMatchers.isNative()));

                    // Advice会在目标方法入口执行，用于检查当前调用栈是否绕过Fire封装API
                    AgentBuilder.Transformer.ForAdvice advice = newAdviceTransformer(TraceStandardAdvice.class)
                            .advice(methodMatcher, TraceStandardAdvice.class.getName());

                    resettable = newDefaultAgentBuilder()
                            .type(ElementMatchers.namedOneOf(distinctClassNames(sourcePatterns)))
                            .transform(advice)
                            .installOn(instrumentation);

                    logger.warn(String.format("Trace standard 代码扫描服务已启动：api=%s durationMin=%d autoExit=%s stackScanDepth=%d",
                            displayMappings(mappings), durationMin, FireFrameworkConf.traceCodeStandardAutoExit(), FireFrameworkConf.traceCodeStandardStackScanDepth()));
                } catch (Throwable e) {
                    // 安装失败时回滚状态，保证后续可再次尝试启动
                    started.compareAndSet(true, false);
                    sourcePatternMap = Collections.emptyMap();
                    apiMappingMap.clear();
                    shouldAnalyzeMap.clear();
                    logger.error("Trace standard 代码扫描服务启动失败", e);
                }
            }
        }
    }

    /**
     * 判断当前方法是否需要执行标准化分析，按「被增强类#方法名」去重，同一个原生API方法只分析一次。
     *
     * @param declaringType 当前被增强方法所属类名
     * @param methodName    当前被增强方法名
     * @return true表示首次命中且仍处于扫描窗口，需要继续分析
     */
    public static boolean shouldAnalyze(String declaringType, String methodName) {
        if (!canScan()) {
            return false;
        }

        // 构造方法级去重key，避免同一个原生API方法被重复分析
        String methodKey = declaringType + "." + methodName;
        return shouldAnalyzeMap.putIfAbsent(methodKey, Boolean.TRUE) == null;
    }

    /**
     * 打印原生API调用日志：当调用栈未出现Fire封装包时，输出疑似业务直连原生API的位置
     *
     * @param declaringType 当前被增强方法所属类名
     * @param methodName    当前被增强方法名
     */
    public static void analyzeCodeStandard(String declaringType, String methodName) {
        // 根据当前被增强的类与方法解析其对应的source/target配置
        TraceStandardApi mapping = resolveMapping(declaringType, methodName);
        if (mapping == null) {
            return;
        }

        // 如果调用栈中已经走到Fire封装包，认为是合规调用；否则返回首个疑似业务调用方
        StackTraceElement caller = firstIllegalCaller(mapping, declaringType);
        if (caller == null) {
            return;
        }

        logger.warn(String.format("[TraceStandard] 检测到代码正在使用原生API：source=%s method=%s.%s；Fire已提供封装后的API，建议使用：%s；调用位置：class=%s method=%s line=%d",
                mapping.getSource(), declaringType, methodName, mapping.getTarget(), caller.getClassName(), caller.getMethodName(), caller.getLineNumber()));

        // autoExit开启时，第一次命中后关闭扫描状态，后续Advice快速return，降低长期运行开销
        if (FireFrameworkConf.traceCodeStandardAutoExit() && started.compareAndSet(true, false)) {
            logger.warn("[TraceStandard] 已命中不合法API调用，autoExit=true，后续检测将快速返回");
        }
    }

    /**
     * 判断当前是否需要继续执行规范检测逻辑
     *
     * @return true表示继续分析，false表示Advice快速返回
     */
    private static boolean canScan() {
        if (!started.get()) {
            return false;
        }

        // 超过配置的分析时长后关闭扫描状态，但不卸载ByteBuddy transformer，避免影响其他增强能力
        if (System.nanoTime() > endNanos) {
            if (started.compareAndSet(true, false)) {
                logger.warn("[TraceStandard] 已超过配置的分析时长，后续检测将快速返回");
            }
            return false;
        }

        return true;
    }

    /**
     * 从配置项 {@code fire.trace.codeStandard.api} 中解析原生API与Fire封装API的映射关系
     *
     * @return 规范检测API映射列表，配置为空时返回空列表
     */
    private static List<TraceStandardApi> buildApiMappingsFromFireConf() {
        String json = FireFrameworkConf.traceCodeStandardApi();
        if (StringUtils.isBlank(json)) {
            return Collections.emptyList();
        }

        try {
            // 配置格式为JSON数组，source为「全限定类名.方法名」或「全限定类名.*」
            return JSONUtils.newObjectMapperWithDefaultConf().readValue(json, new TypeReference<List<TraceStandardApi>>() {});
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 将配置列表转换为 pattern -> mapping 映射，并过滤无效 pattern
     */
    private static Map<String, TraceStandardApi> buildSourcePatternMap(List<TraceStandardApi> mappings) {
        Map<String, TraceStandardApi> patternMap = new LinkedHashMap<>();
        for (TraceStandardApi mapping : mappings) {
            if (mapping == null || StringUtils.isBlank(mapping.getSource())) {
                continue;
            }

            String sourcePattern = mapping.getSource().trim();
            try {
                splitClassAndMethod(sourcePattern);
                patternMap.put(sourcePattern, mapping);
            } catch (IllegalArgumentException e) {
                logger.warn("忽略无效的 Trace standard api 配置: " + sourcePattern);
            }
        }
        return Collections.unmodifiableMap(patternMap);
    }

    /**
     * 根据被增强的类名与方法名解析对应的API映射，支持精确匹配与「类.*」通配符
     *
     * @param declaringType 当前被增强方法所属类名
     * @param methodName    当前被增强方法名
     * @return 匹配到的API映射，未匹配返回null
     */
    private static TraceStandardApi resolveMapping(String declaringType, String methodName) {
        if (StringUtils.isBlank(declaringType) || StringUtils.isBlank(methodName)) {
            return null;
        }

        String cacheKey = declaringType + "#" + methodName;
        return apiMappingMap.computeIfAbsent(cacheKey, key -> lookupMapping(declaringType, methodName));
    }

    /**
     * 按「类.方法」优先、再回退「类.*」查询映射配置
     */
    private static TraceStandardApi lookupMapping(String declaringType, String methodName) {
        TraceStandardApi exact = sourcePatternMap.get(declaringType + "." + methodName);
        if (exact != null) {
            return exact;
        }
        return sourcePatternMap.get(declaringType + ".*");
    }

    /**
     * 查找第一个疑似业务调用方；如果调用栈中已经包含Fire封装包，则认为是合规调用并返回null
     *
     * @param standardApi       当前命中的API映射
     * @param declaringType 当前被增强方法所属类名
     * @return 疑似业务调用栈帧，合规调用或未找到时返回null
     */
    private static StackTraceElement firstIllegalCaller(TraceStandardApi standardApi, String declaringType) {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        // 限制扫描深度，避免过深调用栈带来额外开销
        int limit = Math.min(stackTrace.length, FireFrameworkConf.traceCodeStandardStackScanDepth());

        // 单次遍历同时完成两项判断：栈中是否经过Fire封装包、首个疑似业务调用方
        boolean hasTarget = false;
        StackTraceElement candidate = null;
        for (int i = 0; i < limit; i++) {
            StackTraceElement element = stackTrace[i];
            String className = element.getClassName();

            // 栈中任意位置出现target包即视为合规，不能仅凭浅层业务帧判定违规
            if (className.startsWith(standardApi.getTarget())) {
                hasTarget = true;
            }

            // 记录第一个非忽略栈帧，作为疑似业务直连原生API的调用位置
            if (candidate == null && !isIgnoredStackClass(className, standardApi, declaringType)) {
                candidate = element;
            }
        }

        return hasTarget ? null : candidate;
    }

    /**
     * 判断调用栈中的类是否应被忽略，避免日志定位到JDK、ByteBuddy、框架或原生API自身
     *
     * @param className     调用栈帧类名
     * @param mapping       当前命中的API映射
     * @param declaringType 当前被增强方法所属类名
     * @return true表示忽略该栈帧
     */
    private static boolean isIgnoredStackClass(String className, TraceStandardApi mapping, String declaringType) {
        String sourceClass = parseClassName(mapping.getSource());
        return className.startsWith("java.")
                || className.startsWith("javax.")
                || className.startsWith("jdk.")
                || className.startsWith("sun.")
                || className.startsWith("net.bytebuddy.")
                || className.equals(TraceStandardAdvice.class.getName())
                || className.equals(TraceStandardManager.class.getName())
                || className.equals(declaringType)
                || className.equals(sourceClass)
                || isApiStackClass(className, sourceClass)
                || className.startsWith(mapping.getTarget());
    }

    /**
     * 判断调用栈类是否属于source API体系，包含source本身、实现类和子类
     *
     * @param className       调用栈帧类名
     * @param sourceClassName source pattern 对应的全限定类名
     * @return true表示该类属于source API体系
     */
    private static boolean isApiStackClass(String className, String sourceClassName) {
        Class<?> stackClass = loadClass(className);
        Class<?> sourceClass = loadClass(sourceClassName);
        return stackClass != null && sourceClass != null && sourceClass.isAssignableFrom(stackClass);
    }

    /**
     * 将API映射列表格式化为日志展示字符串
     *
     * @param mappings API映射列表
     * @return source->target形式的逗号分隔字符串
     */
    private static String displayMappings(List<TraceStandardApi> mappings) {
        return mappings.stream()
                .map(mapping -> mapping.getSource() + "->" + mapping.getTarget())
                .collect(Collectors.joining(","));
    }
}
