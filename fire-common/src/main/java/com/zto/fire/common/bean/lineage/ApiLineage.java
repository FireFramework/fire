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

package com.zto.fire.common.bean.lineage;

import java.io.Serializable;
import java.util.Objects;

/**
 * 任务运行时采集到的单条 Fire API 使用血缘。
 * 随 {@link Lineage} 发往 Kafka；完整变更史见框架内 {@code ApiMetaRegistry}，不放入本消息。
 *
 * @author ChengLong
 * @since 3.0.0
 */
public class ApiLineage implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * API 名称，如 jdbcUpdateBatch
     */
    private String name;

    /**
     * 所属模块，如 JDBC、Streaming、HBase
     */
    private String module;

    /**
     * 该 API 首次引入的 fire 版本
     */
    private String sinceVersion;

    /**
     * 本任务首次观测到该 API 调用的时间戳（毫秒）
     */
    private Long firstSeen;

    public ApiLineage() {
    }

    public ApiLineage(String name, String module, String sinceVersion, Long firstSeen) {
        this.name = name;
        this.module = module;
        this.sinceVersion = sinceVersion;
        this.firstSeen = firstSeen;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getModule() {
        return module;
    }

    public void setModule(String module) {
        this.module = module;
    }

    public String getSinceVersion() {
        return sinceVersion;
    }

    public void setSinceVersion(String sinceVersion) {
        this.sinceVersion = sinceVersion;
    }

    public Long getFirstSeen() {
        return firstSeen;
    }

    public void setFirstSeen(Long firstSeen) {
        this.firstSeen = firstSeen;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        ApiLineage that = (ApiLineage) object;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "ApiLineage{" +
                "name='" + name + '\'' +
                ", module='" + module + '\'' +
                ", sinceVersion='" + sinceVersion + '\'' +
                ", firstSeen=" + firstSeen +
                '}';
    }
}
