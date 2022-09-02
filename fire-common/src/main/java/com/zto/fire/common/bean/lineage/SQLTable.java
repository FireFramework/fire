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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 用于封装采集到SQL的实时血缘信息：SQL中所用到的表信息
 *
 * @author ChengLong 2022-09-01 13:32:03
 * @since 2.3.2
 */
public class SQLTable {

    /**
     * Hive、Kafka、JDBC等
     */
    private String catalog;

    /**
     * catalog集群信息url
     */
    private String cluster;

    /**
     * 物理表名
     */
    private String physicalTable;

    /**
     * 在spark或flink中注册成的临时表名
     */
    private String tmpView;

    /**
     * sql中的属性信息，比如with字句的options
     */
    private Map<String, String> options;

    /**
     * 任务中对该表的操作：SELECT、DROP、CREATE等
     */
    private List<String> operation;

    /**
     * 使用到的字段列表，包括字段的名称与类型
     */
    private List<SQLTableColumns> columns;

    public SQLTable() {
        this.operation = new LinkedList<>();
        this.columns = new LinkedList<>();
        this.options = new HashMap<>();
    }

    public SQLTable(String catalog, String cluster, String physicalTable, String tmpView,
                    HashMap<String, String> options, List<String> operation, List<SQLTableColumns> columns) {
        this.catalog = catalog;
        this.cluster = cluster;
        this.physicalTable = physicalTable;
        this.tmpView = tmpView;
        this.options = options;
        this.operation = operation;
        this.columns = columns;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public String getCatalog() {
        return catalog;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public String getCluster() {
        return cluster;
    }

    public void setPhysicalTable(String physicalTable) {
        this.physicalTable = physicalTable;
    }

    public String getPhysicalTable() {
        return physicalTable;
    }

    public void setTmpView(String tmpView) {
        this.tmpView = tmpView;
    }

    public String getTmpView() {
        return tmpView;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    public void setOptions(Map<String, String> options) {
        this.options = options;
    }

    public void setOperation(List<String> operation) {
        this.operation = operation;
    }

    public List<String> getOperation() {
        return operation;
    }

    public void setColumns(List<SQLTableColumns> columns) {
        this.columns = columns;
    }

    public List<SQLTableColumns> getColumns() {
        return columns;
    }

}