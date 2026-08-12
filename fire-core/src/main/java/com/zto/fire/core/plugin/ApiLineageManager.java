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
import com.zto.fire.common.lineage.ApiMetaRegistry;
import com.zto.fire.common.lineage.LineageManager;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.agent.builder.ResettableClassFileTransformer;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.instrument.Instrumentation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Fire API 血缘 ByteBuddy 织入管理器
 *
 * @author ChengLong
 * @since 3.0.2
 */
public final class ApiLineageManager extends TraceManager {
    private static final Logger logger = LoggerFactory.getLogger(ApiLineageManager.class);
    private static final String API_ANNOTATION = "com.zto.fire.common.anno.API";
    private static final AtomicBoolean started = new AtomicBoolean(false);
    private static volatile ResettableClassFileTransformer resettable;

    /** pattern(class.method) → apiName（注解 value，空则方法名） */
    private static final ConcurrentHashMap<String, String> apiNameByPattern = new ConcurrentHashMap<>();

    private ApiLineageManager() {
    }

    /**
     * 按配置安装 API 血缘织入（BaseFire.boot 冷启动；Flink 过早 boot 时可能尚无 pattern）
     */
    public static void startApiLineage() {
        if (!FireFrameworkConf.lineageEnable() || !FireFrameworkConf.lineageApiEnable()) {
            logger.warn("API 血缘未启用，跳过 ByteBuddy 织入：lineageEnable={} lineageApiEnable={}",
                    FireFrameworkConf.lineageEnable(), FireFrameworkConf.lineageApiEnable());
            return;
        }

        synchronized (ApiLineageManager.class) {
            if (resettable != null) {
                logger.warn("API 血缘织入已启动，忽略重复启动");
                return;
            }
            if (!started.compareAndSet(false, true)) {
                return;
            }

            try {
                Map<String, String> patternMap = discoverAnnotatedPatterns();
                if (patternMap.isEmpty()) {
                    started.compareAndSet(true, false);
                    logger.warn("API 血缘暂未发现可织入方法，等待 ensureStarted() 补齐");
                    return;
                }
                apiNameByPattern.clear();
                apiNameByPattern.putAll(patternMap);
                installTransformer(new ArrayList<>(apiNameByPattern.keySet()));
            } catch (Throwable e) {
                started.compareAndSet(true, false);
                resettable = null;
                logger.error("API 血缘 ByteBuddy 织入启动失败", e);
            }
        }
    }

    /**
     * Flink 业务 init 前调用：过早 boot 未装上时在此安装；已安装则按需重装或 retransform
     */
    public static void ensureStarted() {
        if (!FireFrameworkConf.lineageEnable() || !FireFrameworkConf.lineageApiEnable()) {
            return;
        }

        synchronized (ApiLineageManager.class) {
            try {
                Map<String, String> patternMap = discoverAnnotatedPatterns();
                if (patternMap.isEmpty()) {
                    logger.warn("API 血缘 ensureStarted 仍未发现可织入方法");
                    return;
                }

                if (resettable == null) {
                    started.set(false);
                    startApiLineage();
                    return;
                }

                boolean grown = false;
                for (String key : patternMap.keySet()) {
                    if (!apiNameByPattern.containsKey(key)) {
                        grown = true;
                        break;
                    }
                }
                apiNameByPattern.putAll(patternMap);

                if (grown) {
                    // pattern 变多时旧 matcher 不含新方法名，需卸掉后重装
                    logger.warn("API 血缘 pattern 有新增，重新安装织入，patterns={}", apiNameByPattern.size());
                    resetTransformer(resettable);
                    resettable = null;
                    started.set(false);
                    startApiLineage();
                    return;
                }

                retransformTargets(getInstrumentation(), new ArrayList<>(apiNameByPattern.keySet()));
                logger.warn("API 血缘 ensureStarted 完成，patterns={}", apiNameByPattern.size());
            } catch (Throwable e) {
                logger.warn("API 血缘 ensureStarted 失败: {}", e.toString());
            }
        }
    }

    /**
     * Advice 入口
     */
    public static void onMethodEnter(String declaringType, String methodName) {
        if (StringUtils.isBlank(methodName)) {
            return;
        }
        if (!FireFrameworkConf.lineageEnable() || !FireFrameworkConf.lineageApiEnable()) {
            return;
        }
        String apiName = resolveApiName(declaringType, methodName);
        if (StringUtils.isNotBlank(apiName)) {
            LineageManager.addApiLineage(apiName);
        }
    }

    static String resolveApiName(String declaringType, String methodName) {
        if (StringUtils.isNotBlank(declaringType)) {
            String mapped = apiNameByPattern.get(declaringType + "." + methodName);
            if (StringUtils.isNotBlank(mapped)) {
                return mapped;
            }
            for (Map.Entry<String, String> e : apiNameByPattern.entrySet()) {
                String[] cm = splitClassAndMethod(e.getKey());
                if (!cm[1].equals(methodName)) {
                    continue;
                }
                Class<?> holder = loadClass(cm[0]);
                Class<?> runtime = loadClass(declaringType);
                if (holder != null && runtime != null && holder.isAssignableFrom(runtime)) {
                    return e.getValue();
                }
            }
        }
        String value = findApiLineageValue(declaringType, methodName);
        return StringUtils.isNotBlank(value) ? value.trim() : methodName;
    }

    private static void installTransformer(List<String> patterns) throws Exception {
        Instrumentation instrumentation = installByteBuddyAgent();
        // 仅按方法名匹配（对齐 TraceStandard），避免 isDeclaredBy 漏掉实现类 / Scala 方法
        ElementMatcher.Junction<MethodDescription> methodMatcher = buildApiLineageMethodMatcher(patterns)
                .and(ElementMatchers.not(ElementMatchers.isAbstract()))
                .and(ElementMatchers.not(ElementMatchers.isNative()));
        String[] typeNames = distinctClassNames(patterns);
        ElementMatcher.Junction<TypeDescription> typeMatcher =
                ElementMatchers.namedOneOf(typeNames).or(buildImplementorTypeMatcher(patterns));

        AgentBuilder.Transformer.ForAdvice advice = newAdviceTransformer(ApiLineageAdvice.class)
                .advice(methodMatcher, ApiLineageAdvice.class.getName());

        resettable = newDefaultAgentBuilder(instrumentation)
                .type(typeMatcher)
                .transform(advice)
                .installOn(instrumentation);

        retransformTargets(instrumentation, patterns);
        logger.warn("API 血缘 ByteBuddy 织入已启动（按方法名），patterns={}", patterns.size());
    }

    /**
     * 方法匹配只约束方法名，类型范围由 typeMatcher（holders / 实现类）收口
     */
    private static ElementMatcher.Junction<MethodDescription> buildApiLineageMethodMatcher(List<String> patterns) {
        return patterns.stream()
                .map(pattern -> {
                    String[] classAndMethod = splitClassAndMethod(pattern);
                    ElementMatcher.Junction<MethodDescription> matcher = ElementMatchers.isMethod();
                    if (!"*".equals(classAndMethod[1])) {
                        matcher = matcher.and(ElementMatchers.named(classAndMethod[1]));
                    }
                    return matcher;
                })
                .reduce(ElementMatcher.Junction::or)
                .orElseGet(ElementMatchers::none);
    }

    private static Map<String, String> discoverAnnotatedPatterns() {
        Map<String, String> map = new LinkedHashMap<>();
        List<String> holders = ApiMetaRegistry.holdersJava();
        if (holders == null || holders.isEmpty()) {
            logger.warn("api-lineage.yaml 未配置 holders，跳过 API 血缘织入发现");
            return map;
        }
        for (String className : holders) {
            if (StringUtils.isBlank(className)) {
                continue;
            }
            Class<?> clazz = loadClass(className.trim());
            if (clazz == null) {
                continue;
            }
            for (Method method : clazz.getDeclaredMethods()) {
                String apiName = readApiLineageValue(method);
                if (apiName == null) {
                    continue;
                }
                if (StringUtils.isBlank(apiName)) {
                    apiName = method.getName();
                }
                map.put(className.trim() + "." + method.getName(), apiName.trim());
            }
        }
        return map;
    }

    private static String findApiLineageValue(String declaringType, String methodName) {
        Class<?> clazz = loadClass(declaringType);
        if (clazz == null) {
            return null;
        }
        String value = readApiLineageValue(clazz, methodName);
        if (value != null) {
            return value;
        }
        Class<?> superClass = clazz.getSuperclass();
        while (superClass != null && superClass != Object.class) {
            value = readApiLineageValue(superClass, methodName);
            if (value != null) {
                return value;
            }
            superClass = superClass.getSuperclass();
        }
        return null;
    }

    private static String readApiLineageValue(Class<?> clazz, String methodName) {
        for (Method method : clazz.getDeclaredMethods()) {
            if (!method.getName().equals(methodName)) {
                continue;
            }
            String value = readApiLineageValue(method);
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    /** @return null 无注解；"" 表示 value 为空（使用方法名） */
    private static String readApiLineageValue(Method method) {
        for (Annotation ann : method.getDeclaredAnnotations()) {
            if (!API_ANNOTATION.equals(ann.annotationType().getName())) {
                continue;
            }
            try {
                Object value = ann.annotationType().getMethod("value").invoke(ann);
                return value instanceof String ? (String) value : "";
            } catch (Throwable ignored) {
                return "";
            }
        }
        return null;
    }

    private static ElementMatcher.Junction<TypeDescription> buildImplementorTypeMatcher(List<String> patterns) {
        return patterns.stream()
                .map(TraceManager::parseClassName)
                .distinct()
                .map(className -> ElementMatchers.hasSuperType(ElementMatchers.named(className)))
                .reduce(ElementMatcher.Junction::or)
                .orElseGet(ElementMatchers::none);
    }

    private static void retransformTargets(Instrumentation instrumentation, List<String> patterns) {
        if (instrumentation == null) {
            return;
        }
        Set<String> typeNames = new LinkedHashSet<>();
        for (String pattern : patterns) {
            try {
                typeNames.add(parseClassName(pattern));
            } catch (Throwable ignored) {
                // ignore
            }
        }
        typeNames.add("com.zto.fire.flink.ext.stream.StreamExecutionEnvExt");
        typeNames.add("com.zto.fire.spark.ext.core.SparkSessionExt");

        List<Class<?>> targets = new ArrayList<>();
        for (String typeName : typeNames) {
            Class<?> clazz = loadClass(typeName);
            if (clazz != null && instrumentation.isModifiableClass(clazz)) {
                targets.add(clazz);
            }
        }
        for (Class<?> clazz : instrumentation.getAllLoadedClasses()) {
            if (clazz == null) {
                continue;
            }
            if (typeNames.contains(clazz.getName()) && instrumentation.isModifiableClass(clazz) && !targets.contains(clazz)) {
                targets.add(clazz);
            }
        }
        if (targets.isEmpty()) {
            return;
        }
        try {
            instrumentation.retransformClasses(targets.toArray(new Class<?>[0]));
            logger.warn("API 血缘 retransform 完成，数量={}", targets.size());
        } catch (Throwable e) {
            logger.warn("API 血缘 retransform 部分失败: {}", e.toString());
        }
    }
}
