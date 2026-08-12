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

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;

/**
 * 任务运行时采集到的单条 Fire API 使用血缘
 * 随 Lineage 发往 Kafka完整变更史见框架内 ApiMetaRegistry，不放入本消息
 *
 * @author ChengLong
 * @since 3.0.0
 */
public class ApiLineage implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * API 所在类全限定名，如 com.zto.fire.flink.ext.stream.StreamExecutionEnvExt
     * JSON 字段名为 class（Java 关键字，字段用 clazz）
     */
    @JsonProperty("class")
    private String clazz;

    /**
     * API 名称，如 createRandomLongStream / jdbcUpdateBatch
     */
    private String api;

    /**
     * 所属模块，如 JDBC、Streaming、HBase
     */
    private String module;

    /**
     * 该 API 首次引入的 fire 版本
     */
    private String sinceVersion;

    public ApiLineage() {
    }

    public ApiLineage(String clazz, String api, String module, String sinceVersion) {
        this.clazz = clazz;
        this.api = api;
        this.module = module;
        this.sinceVersion = sinceVersion;
    }

    @JsonProperty("class")
    public String getClazz() {
        return clazz;
    }

    @JsonProperty("class")
    public void setClazz(String clazz) {
        this.clazz = clazz;
    }

    public String getApi() {
        return api;
    }

    public void setApi(String api) {
        this.api = api;
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

    /**
     * 去重键：class#api
     */
    public String identityKey() {
        String c = clazz == null ? "" : clazz.trim();
        String a = api == null ? "" : api.trim();
        return c + "#" + a;
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
        return Objects.equals(clazz, that.clazz) && Objects.equals(api, that.api);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clazz, api);
    }

    @Override
    public String toString() {
        return "ApiLineage{" +
                "class='" + clazz + '\'' +
                ", api='" + api + '\'' +
                ", module='" + module + '\'' +
                ", sinceVersion='" + sinceVersion + '\'' +
                '}';
    }
}
