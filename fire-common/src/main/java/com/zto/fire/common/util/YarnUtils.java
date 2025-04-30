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
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.IOException;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Yarn相关工具类
 * @author ChengLong 2018年8月10日 16:03:29
 */
public class YarnUtils {
    private static final Logger LOG = LoggerFactory.getLogger(YarnUtils.class);
    private static Integer containerPhysicalMemory;

    private YarnUtils() {}
    /**
     * 使用正则提取日志中的applicationId
     * @param log
     * @return
     */
    public static String getAppId(String log) {
        // 正则表达式规则
        String regEx = "application_[0-9]+_[0-9]+";
        // 编译正则表达式
        Pattern pattern = Pattern.compile(regEx);
        // 忽略大小写的写法
        Matcher matcher = pattern.matcher(log);
        // 查找字符串中是否有匹配正则表达式的字符/字符串
        if(matcher.find()) {
            return matcher.group();
        } else {
            return "";
        }
    }

    /**
     * 获取Yarn ContainerId
     *
     * @return ContainerId
     */
    public static Optional<ContainerId> getContainerId() {
        String id = System.getenv("CONTAINER_ID");
        if (StringUtils.isBlank(id)) {
            return Optional.empty();
        }
        return Optional.of(ContainerId.fromString(id));
    }

    /**
     * 获取当前container的物理内存大小
     */
    public static int getContainerPhysicalMemory() {
        if (containerPhysicalMemory != null) {
            return containerPhysicalMemory;
        }

        YarnClient yarnClient = null;
        try {
            Optional<ContainerId> containerId = getContainerId();
            if (!containerId.isPresent()) {
                return 0;
            }

            YarnConfiguration conf = new YarnConfiguration();
            yarnClient = YarnClient.createYarnClient();
            yarnClient.init(conf);
            yarnClient.start();
            containerPhysicalMemory = yarnClient.getContainerReport(containerId.get()).getAllocatedResource().getMemory();
            return containerPhysicalMemory;
        } catch (Exception e) {
            LOG.error("Yarn物理内存获取失败！", e);
        } finally {
            if (yarnClient != null) {
                try {
                    yarnClient.close();
                } catch (IOException e) {
                }
            }
        }

        return 0;
    }

    /**
     * 获取当前container的物理内存上限
     */
    public static long getContainerPmemLimit(double ratio) {
        return (long) (getContainerPhysicalMemory() * ratio);
    }

    /**
     * 获取当前container的虚拟内存的上限
     */
    public static long getContainerVmemLimit(double ratio) {
        return (long) (getContainerPhysicalMemory() * ratio);
    }
}
