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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/**
 * Fire框架工具类，用于与fire框架进行反向push操作
 *
 * @author ChengLong
 * @Date 2024/4/3 10:30
 */
public class FireEngineUtils {
    private static final Logger LOG = LoggerFactory.getLogger(FireEngineUtils.class);
    private static Method addSqlMethod = null;
    private static Method postMethod = null;
    private static Method confMethod = null;

    static {
        Class<?> lineageClass = null;
        Class<?> exceptionBusClass = null;
        Class<?> confClass = null;
        final String lineageClassName = "com.zto.fire.common.lineage.LineageManager";
        final String exceptionClassName = "com.zto.fire.common.util.ExceptionBus";
        final String fireConfClassName = "com.zto.fire.common.util.PropUtils";

        try {
            // 使用标准类加载器尝试加载
            lineageClass = Class.forName(lineageClassName);
            exceptionBusClass = Class.forName(exceptionClassName);
            confClass = Class.forName(fireConfClassName);
        } catch (Throwable t) {
            LOG.warn("使用标准类加载器无法检测到fire相关依赖");
        }

        // 尝试使用当前线程类加载器进行加载
        try {
            ClassLoader currentLoader = Thread.currentThread().getContextClassLoader();
            if (lineageClass == null) {
                // 加载sql血缘采集类
                lineageClass = currentLoader.loadClass(lineageClassName);
            }

            if (exceptionBusClass == null) {
                // 加载根因分析采集类
                exceptionBusClass = currentLoader.loadClass(exceptionClassName);
            }

            if (confClass == null) {
                // 加载fire框架配置类
                confClass = currentLoader.loadClass(fireConfClassName);
            }
        } catch (Throwable t) {
            LOG.warn("使用当前线程加载器无法检测到fire相关依赖");
        }

        // 加载fire框架相关方法
        try {
            if (lineageClass != null) {
                addSqlMethod = lineageClass.getMethod("addSql", String.class);
            }

            if (exceptionBusClass != null) {
                postMethod = exceptionBusClass.getMethod("post", Throwable.class, String.class);
            }

            if (confClass != null) {
                confMethod = confClass.getMethod("getProperty", String.class);
            }

            LOG.info("Fire框架相关依赖加载成功!");
        } catch (Throwable t) {
            LOG.error("无法检测到fire相关依赖，请集成fire框架并升级至最新版本！！！", t);
        }
    }

    /**
     * 采集SQL并push给fire框架
     *
     * @param sql
     * sql脚本
     */
    public static void addSql(String sql) {
        try {
            if (addSqlMethod != null) {
                addSqlMethod.invoke(null, sql);
                LOG.info("Push sql to fire framework:\n" + sql.substring(0, Math.min(sql.length() - 1, 100)));
            }
        } catch (Exception e) {
        }
    }

    /**
     * 采集异常堆栈并push给fire框架
     */
    public static void postException(Throwable t, String sql) {
        try {
            if (postMethod != null) {
                postMethod.invoke(null, t, sql == null ? "" : sql);
                LOG.debug("Push exception to fire framework.", t);
            }
        } catch (Exception e) {

        }
    }

    /**
     * 采集异常堆栈并push给fire框架
     */
    public static void postException(Throwable t) {
        postException(t, "");
    }

    /**
     * 获取fire框架中的配置信息
     * @param key
     * 配置的key
     * @return
     * 配置的value
     */
    public static String getStringConf(String key) {
        if (StringUtils.isBlank(key)) return "";

        try {
            if (confMethod != null) {
                return (String) confMethod.invoke(null, key);
            }
        } catch (Exception e) {
        }

        return "";
    }

    /**
     * 获取fire框架中的boolean类型的配置信息
     */
    public static boolean getBooleanConf(String key) {
        String value = getStringConf(key);
        if (StringUtils.isBlank(value)) return false;
        return Boolean.parseBoolean(value);
    }

    /**
     * 获取fire框架中的int类型的配置信息
     */
    public static int getIntConf(String key) {
        String value = getStringConf(key);
        if (StringUtils.isBlank(value)) return -1;
        return Integer.parseInt(value);
    }

    /**
     * 获取fire框架中的long类型的配置信息
     */
    public static long getLongConf(String key) {
        String value = getStringConf(key);
        if (StringUtils.isBlank(value)) return -1;
        return Long.parseLong(value);
    }
}