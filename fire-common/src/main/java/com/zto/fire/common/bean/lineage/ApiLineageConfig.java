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

import java.util.ArrayList;
import java.util.List;

/**
 * 用于api血缘变更日志反序列化
 *
 * @author ChengLong
 * @since 3.0.2
 */
public class ApiLineageConfig {

    private List<ApiMetaItem> apis = new ArrayList<>();

    public List<ApiMetaItem> getApis() {
        return apis;
    }

    public void setApis(List<ApiMetaItem> apis) {
        this.apis = apis != null ? apis : new ArrayList<>();
    }

    /**
     * 单条 API 元数据（对应 yaml 中 apis 列表元素）
     */
    public static class ApiMetaItem {
        private String name;
        private String module;
        private String sinceVersion;
        private List<String> engines;
        private List<ApiChangeItem> changes;

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

        public List<String> getEngines() {
            return engines;
        }

        public void setEngines(List<String> engines) {
            this.engines = engines;
        }

        public List<ApiChangeItem> getChanges() {
            return changes;
        }

        public void setChanges(List<ApiChangeItem> changes) {
            this.changes = changes;
        }
    }

    /**
     * API 变更记录
     */
    public static class ApiChangeItem {
        private String version;
        private String date;
        private String summary;

        public String getVersion() {
            return version;
        }

        public void setVersion(String version) {
            this.version = version;
        }

        public String getDate() {
            return date;
        }

        public void setDate(String date) {
            this.date = date;
        }

        public String getSummary() {
            return summary;
        }

        public void setSummary(String summary) {
            this.summary = summary;
        }
    }
}
