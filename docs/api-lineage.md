<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Fire API 血缘 — 实时平台对接需求说明

> **文档受众**：实时平台研发 / 数据产品  
> **消息来源**：Fire 框架任务运行时采集，经 Kafka 上报（与现有数据源血缘同一 Topic、同一消息信封）  
> **适用版本**：Fire ≥ 3.0.2（`apis` 字段新增；旧消费者可忽略该字段保持兼容）

---

## 1. 背景与目标

### 1.1 要解决什么问题

Fire 对外提供大量封装 API（JDBC / HBase / Streaming 等）。框架升级、API 行为变更或下线时，平台侧往往只能「全量灰度 / 全量回归」，无法精确知道：

- 某个 API 被哪些线上任务实际调用？
- 改动该 API 的影响面有多大？
- 灰度名单应如何圈定，才能做到**小影响面、高有效性**？

### 1.2 API 血缘的作用

Fire 在任务运行时采集「本任务实际调用过的 Fire API」，并随血缘消息上报。平台消费后可构建：

**API → 任务** 的反向索引，以及 **任务 → API 列表** 的正向视图。

从而支持：

| 能力 | 说明 |
|------|------|
| 影响面分析 | 改某个 API 时，精确列出受影响的 `platformAppId` / 任务 |
| 精准灰度 | 仅对使用了目标 API 的任务做灰度，避免无关任务掺入 |
| 灰度有效性 | 灰度包覆盖「真实调用方」，回归更有针对性 |
| 版本兼容评估 | 结合 `fireVersion`、`apis[].sinceVersion` 评估升级风险 |

> 采集原则：仅记录**运行时真实调用过**的 API（首次调用写入，按 `class#api` 去重），不是扫描源码静态列表。

---

## 2. 平台侧功能需求（建议）

### 2.1 必做

1. **消费血缘 Topic**，解析根级字段 `apis`（数组）；兼容历史消息无该字段或为 `null`。
2. **落库 / 索引**：以任务维度存储 API 使用快照；支持按时间覆盖更新（更新规则同已有的实时血缘）。
3. **按 API 搜索任务**：输入 API 名或 `class + api`，展示使用该 API 的任务列表（至少含 `platformAppId`、`appId`、`mainClass`、`engine`、`fireVersion`）。
4. **按任务查看 API**：任务详情页展示该任务上报的 `apis` 列表。
5. **任务唯一关联**：优先使用 `platformAppId`（任务 ID）；缺失时回退 `appId` + `mainClass`。

---

## 3. 数据采集与上报（对接侧需知）

```
任务调用带 @API 的方法
        │
        ▼
ByteBuddy 织入采集 → 内存去重（class#api）
        │
        ▼
组装 Lineage（FireTask + datasource + sql + apis）
        │
        ▼  fire.lineage.send.mq.enable=true
Kafka（与数据源血缘同一 Topic）
```

| 项 | 说明 |
|----|------|
| 开关 | `fire.lineage.enable=true` 且 `fire.lineage.api.enable=true` |
| 发送 | `fire.lineage.send.mq.enable=true`，配置 `url` / `topic` |
| 发送节奏 | Spark 约每 10s；Flink 约每 120s；进程退出前再发一次 |
| 语义 | **累计快照**：每次消息携带当前已采集到的全部 `apis`，非单次调用事件流 |
| 上限 | `fire.lineage.max.size`（默认 500，含 API 条目规模控制） |

任务侧参考配置：

```properties
# 以下配置默认均已开启
fire.lineage.enable=true
fire.lineage.api.enable=true
fire.lineage.send.mq.enable=true
fire.lineage.send.mq.url=broker1:9092,broker2:9092
# 示例 Topic 名，以环境实际为准
fire.lineage.send.mq.topic=sjzn_platform_realtime_lineage
```

---

## 4. 消息结构总览

对外 JSON 类型：`com.zto.fire.common.bean.lineage.Lineage`（继承 `FireTask`）。

根级字段关系：

```text
Lineage
├── 任务标识（FireTask 字段）
├── datasource   // 数据源血缘（既有）
├── sql          // SQL 血缘（既有）
└── apis[]       // API 血缘（新增，与 datasource 同级）
```

### 4.1 任务标识字段（FireTask）

| 字段 | 类型 | 说明 |
|------|------|------|
| `engine` | string | `spark` / `flink` |
| `engineVersion` | string | 引擎版本 |
| `fireVersion` | string | Fire 框架版本 |
| `ip` / `hostname` / `pid` | string | 上报端 JVM 主机信息 |
| `mainClass` | string | 任务主类全限定名 |
| `timestamp` | string | 本条消息构造时间 |
| `launchTime` | string | 任务启动时间 |
| `uptime` | long | 已运行毫秒 |
| `appId` | string | YARN / 应用 ID |
| `deployMode` | string | 提交模式 |
| `jobType` | string | streaming / batch 等 |
| `platformAppId` | string | **实时平台任务 ID**（`fire.config_center.app.id`） |

### 4.2 `apis[]` 元素（ApiLineage）— 平台核心字段

| 字段 | JSON Key | 类型 | 说明 |
|------|----------|------|------|
| clazz | **`class`** | string | API 声明类全限定名 |
| api | `api` | string | API 名称（与 `@API` 一致，空则方法名） |
| module | `module` | string | 模块：`JDBC` / `HBase` / `Streaming` / `UNKNOWN` 等 |
| sinceVersion | `sinceVersion` | string | 该 API 首次引入的 Fire 版本；未登记元数据时可为空串 |

### 4.3 `datasource` / `sql`（既有，简述）

- `datasource`：`Map`，key 为数据源枚举名（如 `JDBC`、`HBASE`、`KAFKA`），value 为描述对象数组。
- `sql`：含 `statements`、`tables`、`relations`。

平台若已消费数据源血缘，仅需增量解析 `apis`。

---

## 5. 完整血缘 JSON 样本

以下为一条**完整** Kafka 消息示例（字段与框架序列化一致；集群地址等为示例值）。平台对接请以本结构为准。

```json
{
  "engine": "flink",
  "engineVersion": "1.19.x",
  "fireVersion": "3.0.2",
  "ip": "10.12.34.56",
  "hostname": "hadoop-tm-01",
  "pid": "28461",
  "mainClass": "com.zto.fire.examples.flink.lineage.ApiLineageTest",
  "timestamp": "2026-09-18 09:40:01",
  "launchTime": "2026-09-18 09:35:00",
  "uptime": 301000,
  "appId": "application_1726612345678_0123",
  "deployMode": "yarn-application",
  "jobType": "STREAMING",
  "platformAppId": "100086",

  "datasource": {
    "JDBC": [
      {
        "datasource": "mysql",
        "cluster": "jdbc:mysql://mysql-server:3306/fire?useSSL=true",
        "tableName": "spark_test",
        "username": "root",
        "operation": ["SELECT"]
      }
    ],
    "HBASE": [
      {
        "datasource": "hbase",
        "cluster": "hbase-zk.example.com:2181",
        "tableName": "fire_test_13",
        "username": "",
        "operation": ["GET"]
      }
    ],
    "KAFKA": [
      {
        "datasource": "kafka",
        "cluster": "broker1:9092,broker2:9092",
        "topics": "ods_order_event",
        "groupId": "fire_lineage_demo_gid",
        "operation": ["SOURCE"]
      }
    ],
    "HIVE": [
      {
        "datasource": "hive",
        "cluster": "thrift://hms.example.com:9083",
        "tableName": "dw.dwd_order_di",
        "username": "",
        "partitions": [
          {
            "name": "dt",
            "value": "20260918"
          }
        ],
        "operation": ["SELECT"]
      }
    ]
  },

  "sql": {
    "statements": [
      "select * from spark_test where age>=?"
    ],
    "tables": [
      {
        "catalog": "jdbc",
        "cluster": "jdbc:mysql://mysql-server:3306/fire?useSSL=true",
        "physicalTable": "spark_test",
        "connector": "jdbc",
        "tmpView": null,
        "comment": null,
        "options": {},
        "operation": ["SELECT"],
        "columns": [
          {
            "name": "id",
            "type": "INT"
          },
          {
            "name": "name",
            "type": "VARCHAR"
          },
          {
            "name": "age",
            "type": "INT"
          }
        ],
        "partitions": [],
        "primaryKey": [],
        "partitionField": []
      }
    ],
    "relations": [
      {
        "srcTable": "spark_test",
        "sinkTable": "dwd_student_sink",
        "columnsRelations": [
          {
            "sourceColumn": "id",
            "targetColumn": "id"
          },
          {
            "sourceColumn": "name",
            "targetColumn": "user_name"
          }
        ]
      }
    ]
  },

  "apis": [
    {
      "class": "com.zto.fire.flink.ext.provider.JdbcFlinkProvider",
      "api": "jdbcQueryList",
      "module": "JDBC",
      "sinceVersion": "2.0.0"
    },
    {
      "class": "com.zto.fire.flink.ext.provider.HBaseConnectorProvider",
      "api": "hbaseGetListAsync2",
      "module": "HBase",
      "sinceVersion": "2.0.0"
    },
    {
      "class": "com.zto.fire.flink.ext.stream.StreamExecutionEnvExt",
      "api": "createRandomLongStream",
      "module": "Streaming",
      "sinceVersion": "2.0.0"
    }
  ]
}
```

### 5.1 仅关注 API 搜索时的最小字段集

平台若只做「搜 API → 列任务」，至少持久化：

```json
{
  "platformAppId": "100086",
  "appId": "application_1726612345678_0123",
  "mainClass": "com.zto.fire.examples.flink.lineage.ApiLineageTest",
  "engine": "flink",
  "fireVersion": "3.0.2",
  "timestamp": "2026-09-18 09:40:01",
  "apis": [
    {
      "class": "com.zto.fire.flink.ext.provider.JdbcFlinkProvider",
      "api": "jdbcQueryList",
      "module": "JDBC",
      "sinceVersion": "2.0.0"
    }
  ]
}
```

搜索示例：

- 关键字 `jdbcQueryList` → 命中上例任务 `platformAppId=100086`
- 关键字 `com.zto.fire.flink.ext.provider.HBaseConnectorProvider#hbaseGetListAsync2` → 精确命中

---

## 6. 页面与交互建议（产品）

1. **API 搜索页**  
   - 输入：API 名 / 类名 / `class#api` / 模块  
   - 输出：任务列表
2. **任务详情 — API 血缘 Tab**  
   - 展示该任务全部 `apis`，支持按 module 分组  

---

## 7. 联调与示例任务

框架内置示例（开启 debug 打印血缘 JSON）：

- Flink：`fire-examples/flink-examples/.../lineage/ApiLineageTest.scala`
- Spark：`fire-examples/spark-examples/.../lineage/ApiLineageTest.scala`

联调建议：

```properties
fire.lineage.enable=true
fire.lineage.api.enable=true
fire.lineage.debug.print=true
fire.lineage.send.mq.enable=true
fire.lineage.send.mq.url=<联调 Kafka>
fire.lineage.send.mq.topic=<联调 Topic>
```
