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

package com.zto.fire.common.enu;

import com.google.common.collect.Maps;
import com.zto.fire.common.lineage.parser.connector.*;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * 数据源类型
 *
 * @author ChengLong
 * @create 2020-07-07 16:36
 * @since 2.0.0
 */
public enum Datasource {
    // TODO: 添加新的数据源时务必在static代码块中添加与DatasourceDesc子类的映射关系
    HIVE(1), HBASE(2), KAFKA(3), ROCKETMQ(4), REDIS(5),
    ELASTICSEARCH(6), MYSQL(7), TIDB(8), ORACLE(9), SQLSERVER(10),
    DB2(11), CLICKHOUSE(12), PRESTO(13), KYLIN(14), DERBY(15),
    VIEW(16), JDBC(17), FIRE_ROCKETMQ(18), PostgreSQL(19),
    CUSTOMIZE_SOURCE(20), CUSTOMIZE_SINK(21), HUDI(22), DORIS(23),
    ICEBERG(24), PAIMON(25), MONGODB(26), PRINT(27), DATAGEN(28),
    FILESYSTEM(29), BLACKHOLE(30), UNKNOWN(404);

    private static Map<Datasource, Class<?>> datasourceMap = Maps.newHashMap();

    static {
        // 将数据源信息归类，新增数据源务必在此处维护，否则会导致Flink引擎解析不到
        datasourceMap.put(JDBC, DBDatasource.class);
        datasourceMap.put(PostgreSQL, DBDatasource.class);
        datasourceMap.put(MYSQL, DBDatasource.class);
        datasourceMap.put(TIDB, DBDatasource.class);
        datasourceMap.put(ORACLE, DBDatasource.class);
        datasourceMap.put(SQLSERVER, DBDatasource.class);
        datasourceMap.put(DB2, DBDatasource.class);
        datasourceMap.put(CLICKHOUSE, DBDatasource.class);
        datasourceMap.put(PRESTO, DBDatasource.class);
        datasourceMap.put(KYLIN, DBDatasource.class);
        datasourceMap.put(DERBY, DBDatasource.class);
        datasourceMap.put(HBASE, DBDatasource.class);
        datasourceMap.put(REDIS, DBDatasource.class);
        datasourceMap.put(MONGODB, DBDatasource.class);
        datasourceMap.put(DORIS, DBDatasource.class);
        datasourceMap.put(ELASTICSEARCH, DBDatasource.class);

        // 文件类
        datasourceMap.put(HIVE, HiveDatasource.class);
        datasourceMap.put(HUDI, HudiDatasource.class);
        datasourceMap.put(FILESYSTEM, FileDatasource.class);
        datasourceMap.put(ICEBERG, FileDatasource.class);
        datasourceMap.put(PAIMON, PaimonDatasource.class);

        // 消息队列类别
        datasourceMap.put(KAFKA, MQDatasource.class);
        datasourceMap.put(ROCKETMQ, MQDatasource.class);
        datasourceMap.put(FIRE_ROCKETMQ, MQDatasource.class);

        // 自定义connector
        datasourceMap.put(CUSTOMIZE_SOURCE, CustomizeDatasource.class);
        datasourceMap.put(CUSTOMIZE_SINK, CustomizeDatasource.class);

        // 虚拟connector
        datasourceMap.put(DATAGEN, VirtualDatasource.class);
        datasourceMap.put(PRINT, VirtualDatasource.class);
        datasourceMap.put(BLACKHOLE, VirtualDatasource.class);

        // 待归类
        // VIEW / ICEBERG / Paimon / ES
    }

    Datasource(int type) {
    }

    public static Class<?> toDatasource(Datasource datasource) {
        return datasourceMap.get(datasource);
    }

    /**
     * 将字符串解析成指定的枚举类型
     */
    public static Datasource parse(String dataSource) {
        if (StringUtils.isBlank(dataSource)) return UNKNOWN;
        try {
            String trimDatasource = dataSource.replace("-", "_");
            return Enum.valueOf(Datasource.class, trimDatasource.trim().toUpperCase());
        } catch (Exception e) {
            return UNKNOWN;
        }
    }

    @Override
    public String toString() {
        return this.name();
    }
}
