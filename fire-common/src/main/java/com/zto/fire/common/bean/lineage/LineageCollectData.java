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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 分布式血缘采集载荷：在原有 datasource 基础上增加 apis。
 * <p>
 * 用于 Spark 累加器 / Flink REST 在 container 与 master 之间同步。
 * Kafka 对外消息仍序列化 {@link Lineage}（仅新增 apis 字段，兼容旧结构）。
 * </p>
 * <p>
 * payloadVersion=2 表示新格式；旧客户端若只上报 datasource Map（无本包装），master 端需兼容解析。
 * </p>
 *
 * @author ChengLong
 * @since 3.0.0
 */
public class LineageCollectData implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final int PAYLOAD_VERSION = 2;

    /**
     * 载荷版本，用于兼容旧的「仅 datasource Map」上报格式
     */
    private int payloadVersion = PAYLOAD_VERSION;

    /**
     * 数据源血缘，结构与历史累加器一致：Map[Datasource, Set[DatasourceDesc]]
     */
    private ConcurrentHashMap datasource = new ConcurrentHashMap();

    /**
     * API 使用血缘（按 name 去重后的列表）
     */
    private List<ApiLineage> apis = new ArrayList<>();

    public LineageCollectData() {
    }

    public int getPayloadVersion() {
        return payloadVersion;
    }

    public void setPayloadVersion(int payloadVersion) {
        this.payloadVersion = payloadVersion;
    }

    public ConcurrentHashMap getDatasource() {
        return datasource;
    }

    public void setDatasource(ConcurrentHashMap datasource) {
        this.datasource = datasource == null ? new ConcurrentHashMap() : datasource;
    }

    public List<ApiLineage> getApis() {
        return apis;
    }

    public void setApis(List<ApiLineage> apis) {
        this.apis = apis == null ? new ArrayList<>() : apis;
    }

    public boolean isEmpty() {
        return (datasource == null || datasource.isEmpty()) && (apis == null || apis.isEmpty());
    }
}
