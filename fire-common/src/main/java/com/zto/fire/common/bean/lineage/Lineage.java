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

import com.zto.fire.common.bean.FireTask;

import java.util.ArrayList;
import java.util.List;

/**
 * 用于封装采集到的实时血缘信息。
 * <p>
 * 对外 Kafka JSON 仅做字段新增（apis）
 * </p>
 *
 * @author ChengLong 2022-08-30 15:31:32
 * @since 2.3.2
 */
public class Lineage extends FireTask {

    /**
     * 血缘信息
     */
    private Object datasource;

    /**
     * SQL血缘
     */
    private SQLLineage sql;

    /**
     * Fire API 使用血缘（新增字段，旧消费者可忽略）
     *
     * @since 3.0.0
     */
    private List<ApiLineage> apis;

    public Lineage() {
        super();
    }

    public Lineage(Object lineage) {
        super();
        this.datasource = lineage;
    }

    public Lineage(Object lineage, SQLLineage sql) {
        this.datasource = lineage;
        this.sql = sql;
    }

    public Lineage(Object lineage, SQLLineage sql, List<ApiLineage> apis) {
        this.datasource = lineage;
        this.sql = sql;
        this.apis = apis;
    }

    public Object getDatasource() {
        return datasource;
    }

    public void setDatasource(Object datasource) {
        this.datasource = datasource;
    }

    public SQLLineage getSql() {
        return sql;
    }

    public void setSql(SQLLineage sql) {
        this.sql = sql;
    }

    public List<ApiLineage> getApis() {
        return apis;
    }

    public void setApis(List<ApiLineage> apis) {
        this.apis = apis;
    }

    /**
     * 确保 apis 非 null，便于序列化与合并
     */
    public List<ApiLineage> apisOrEmpty() {
        if (this.apis == null) {
            this.apis = new ArrayList<>();
        }
        return this.apis;
    }
}
