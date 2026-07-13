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

# Hudi 读写

　　Apache Hudi 是常用的流式数据湖存储格式。Fire 框架对 Hudi 进行了深度集成，支持 **Spark Streaming 实时入湖**、**DataFrame 一行写入**、**Flink SQL 入湖** 等多种开发模式，并提供 `@Hudi` 注解统一配置 compaction、clustering、索引等调优参数，同时内置 **实时血缘** 解析能力。

## 一、版本支持

Fire 通过 Maven Profile 适配多个 Hudi 版本，构建时需同时指定 **Spark/Flink 引擎 Profile** 与 **Hudi Profile**：

```shell
# 示例：Spark 3.3 + Hudi 0.13
mvn clean install -DskipTests -Pspark-3.3 -Phudi-0.13 -Pscala-2.12

# 示例：Spark 3.0 + Hudi 0.9（默认 hudi.major.version=0.9）
mvn clean install -DskipTests -Pspark-3.0 -Phudi-0.9 -Pscala-2.12
```

| Maven Profile | Hudi 版本 | 说明 |
| --- | --- | --- |
| `hudi-0.8` | 0.8.0 | Spark 2.3 / 2.4 等早期版本 |
| `hudi-0.9` | 0.9.0 | 框架默认版本 |
| `hudi-0.10` | 0.10.1 | — |
| `hudi-0.13` | 0.13.0 | 推荐 Spark 3.x 生产环境 |
| `hudi-1.0.0` | 1.0.0-beta1 | Hudi 1.x 预览 |

**引擎适配说明：**

| 引擎 | 支持方式 |
| --- | --- |
| Spark 2.3.x ~ 3.5.x | `HudiStreaming` 父类、`df.sinkHudi` API、`@Hudi` 注解、DataSource 增强 |
| Flink 1.12.x ~ 1.19.x | Flink SQL `connector='hudi'`，配合 `datasource` 别名引用配置文件 |

> Spark 3.0 及以上使用 `hudi-spark3-bundle`；Spark 2.x 使用 `hudi-spark-bundle`（无 `3` 后缀）。

---

## 二、数据源配置

### 2.1 Spark 任务必选配置

实时入湖任务（继承 `HudiStreaming`）需配置以下参数，可通过 `@Config` 注解或 properties 文件指定：

```properties
hudi.tableName      =   hudi.t_datacloud
hudi.primaryKey     =   id
hudi.precombineKey  =   createTime
hudi.partitionFieldName =   ds
hudi.tmpView          = msg_view
hudi.repartition      = -1
hudi.sink             = true
hudi.retry.onFailure  = 1
```

Hudi 写入 options 通过 **`hudi.options.{key}`** 前缀配置（支持 keyNum 后缀），与 `@Hudi` 注解映射的配置合并后生效，配置文件优先级更高：

```properties
hudi.options.hoodie.cleaner.commits.retained=10
hudi.options.hoodie.cleaner.policy=KEEP_LATEST_FILE_VERSIONS
hudi.options.hoodie.compact.inline.max.delta.commits=2
```

### 2.2 基于 `@Hudi` 注解

```scala
@Hudi(
  parallelism = 10,
  compactCommits = 2,
  value =
    """
      |hoodie.cleaner.commits.retained=10
      |hoodie.cleaner.policy=KEEP_LATEST_FILE_VERSIONS
      |""")
@Hudi2(parallelism = 20, compactCommits = 5)  // 多数据源，keyNum=2
```

`@Hudi` 注解支持的主要参数：

| 注解属性 | 对应 Hudi 配置 | 说明 |
| --- | --- | --- |
| `parallelism` | `hoodie.*.parallelism` 系列 | 统一并行度 |
| `compactCommits` | `hoodie.compact.inline.max.delta.commits` | 几个批次触发 inline compaction |
| `compactSchedule` | `hoodie.compact.schedule.inline` | 仅调度 compaction 计划 |
| `clusteringCommits` | `hoodie.clustering.inline.max.commits` | clustering 触发间隔 |
| `cleanerPolicy` | `hoodie.cleaner.policy` | CLEAN 策略 |
| `cleanerCommitsRetained` | `hoodie.cleaner.commits.retained` | 保留 commit 数 |
| `useHBaseIndex` | `hoodie.index.type=HBASE` | HBase 二级索引 |
| `useRecordIndex` | `hoodie.index.type=RECORD_INDEX` | 记录级索引（> 0.14.0） |
| `value` / `props` | 任意 `hoodie.*` | 透传 Hudi 原生参数 |

### 2.3 Flink SQL `datasource` 别名

Flink 建表语句中通过 `'datasource' = 'hudi_test'` 引用配置文件中的 options，避免在 SQL 中硬编码 path、Hive Metastore 等敏感信息：

```properties
# commons.properties 或任务 properties
flink.sql.with.hudi_test.path=hdfs:///user/hive/warehouse/hudi.db/t_hudi_flink
flink.sql.with.hudi_test.hive_sync.metastore.uris=thrift://localhost:9083
```

SQL 中只需保留业务相关 options，`datasource` 行会被 Fire 自动替换为配置文件中的完整 options 列表。详见 [properties.md](../properties.md#十一flink-引擎参数) 中 `flink.sql.with.{ds}.{option}` 说明。

---

## 三、Spark API 使用

### [3.1 Spark Streaming 实时入湖](../fire-examples/spark-examples/src/main/scala/com/zto/fire/examples/spark/hudi/HudiTest.scala)

继承 **`HudiStreaming`** 父类，框架自动完成：消费 MQ → 解析 JSON → 注册临时表 → Upsert 入湖 → 执行后置 SQL。

```scala
@Config(
  value = """
            |hudi.tableName     =   hudi.t_datacloud
            |hudi.primaryKey    =   id
            |hudi.precombineKey =   createTime
            |hudi.partitionFieldName =   ds
            |""", files = Array("hudi-common.properties"))
@Hive(cluster = "fat")
@Hudi(parallelism = 10, compactCommits = 2, value =
  """
    |hoodie.cleaner.commits.retained=10
    |hoodie.cleaner.policy=KEEP_LATEST_FILE_VERSIONS
    |""")
@Streaming(interval = 20, backpressure = false, parallelism = 10)
@RocketMQ(brokers = "bigdata_test", topics = "datacloud", groupId = "fire")
object HudiTest extends HudiStreaming {

  /** 可选：批次写入前执行建表等 DDL */
  override protected def sqlBefore(tableName: String): String = {
    s"""
       |CREATE TABLE IF NOT EXISTS $tableName (
       |  `id` BIGINT, `name` STRING, `age` INT,
       |  `createTime` STRING, `ds` STRING)
       |USING hudi
       |OPTIONS (`primaryKey` 'id', `type` 'mor', `preCombineField` 'createTime')
       |PARTITIONED BY (ds)
       |""".stripMargin
  }

  /** 必须：将 MQ 消息临时表转换为待写入 Hudi 的 SQL */
  override protected def sqlUpsert(tmpView: String): String = {
    s"""
       |select id, name, age, createTime,
       |  regexp_replace(substr(createTime,0,10),'-','') ds
       |from $tmpView
       |""".stripMargin
  }

  /** 可选：每批次写入后执行 delete / update 等 */
  override protected def sqlAfter(tmpView: String): String = {
    s"delete from ${tableName} where id>90"
  }
}
```

**执行流程：**

1. `before()` 自动设置 `HoodieSparkSessionExtension` 与 Kryo 序列化
2. 执行 `sqlBefore` 建表（可选）
3. 每个 Streaming 批次：`createMQStream` → JSON 解析 → `sqlUpsert` → `sinkHudi`
4. 执行 `sqlAfter` 后置逻辑（可选）

### [3.2 DataFrame 一行写入](../fire-examples/spark-examples/src/main/scala/com/zto/fire/examples/spark/hudi/HudiTest.scala)

适用于批任务或自定义 Streaming 逻辑：

```scala
val df = sql("select id, name, age, createTime, ds from msg_view")
df.sinkHudi(
  hudiTableName = "hudi.t_datacloud",
  recordKey = "id",
  precombineKey = "createTime",
  partition = "ds"
)
// 多集群 / 多套 options
df.sinkHudi("hudi.t_datacloud", "id", "createTime", "ds", keyNum = 2)
```

### [3.3 Spark DataSource 增强](../datasource.md)

通过配置文件驱动 `read.format / write.format / options`，适合平台化参数热更新场景。Hudi 相关配置前缀为 `spark.datasource.options.hoodie.*`，详见 [datasource.md](../datasource.md)。

---

## 四、Flink API 使用

### [4.1 Flink SQL 实时入湖](../fire-examples/flink-examples/src/main/scala/com/zto/fire/examples/flink/connector/hudi/HudiTest.scala)

```scala
@Streaming(interval = 20, disableOperatorChaining = true, parallelism = 2)
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")
object HudiTest extends FlinkStreaming {

  @Process
  def kafkaSource: Unit = {
    sql(
      """
        |create table if not exists `t_hudi_flink`(
        |  id int PRIMARY KEY NOT ENFORCED,
        |  name string, age int, createTime string, ds string
        |) PARTITIONED BY (`ds`)
        |with(
        |  'connector'='hudi',
        |  'datasource' = 'hudi_test',
        |  'table.type'='MERGE_ON_READ',
        |  'hoodie.datasource.write.recordkey.field'='id',
        |  'precombine.field'='createTime',
        |  'hive_sync.enable'='true',
        |  'hive_sync.db'='hudi',
        |  'hive_sync.table'='t_hudi_flink',
        |  'hive_sync.mode'='hms',
        |  'compaction.async.enabled'='false',
        |  'compaction.schedule.enabled'='true',
        |  'compaction.trigger.strategy'='num_commits',
        |  'compaction.delta_commits'='2'
        |)
        |""".stripMargin)

    sql("""CREATE table source (...) with ('connector'='fire-rocketmq', ...)""")
    sql(
      """
        |insert into t_hudi_flink
        |select id, name, age, createTime,
        |  regexp_replace(substr(createTime,0,10),'-','') ds
        |from source
        |""".stripMargin)
  }
}
```

**说明：**

- `'datasource' = 'hudi_test'` 对应 `flink.sql.with.hudi_test.*` 配置，path、Hive URI 等敏感信息放在 commons.properties
- Fire 内置 Hudi **血缘解析**，可配合 `FlinkLineageAccumulatorManager` 采集库表信息
- Compaction 策略、索引类型等遵循 Flink Hudi Connector 原生语义

---

## 五、配置参数汇总

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| hudi.tableName | — | Hudi 表名（Spark Streaming 必选） |
| hudi.primaryKey | — | 主键字段 |
| hudi.precombineKey | — | 预聚合字段 |
| hudi.partitionFieldName | ds | 分区字段（示例代码中亦可见 `hudi.partition`，等效于默认值 `ds`） |
| hudi.tmpView | msg_view | MQ 消息临时视图名 |
| hudi.repartition | -1 | 写入前 repartition 数，≤0 不执行 |
| hudi.sink | true | 是否执行 upsert 写入 |
| hudi.retry.onFailure | 1 | 批次失败重试次数 |
| hudi.options.{key} | — | Spark 写 Hudi options（前缀，支持 keyNum） |
| flink.sql.with.{ds}.{option} | — | Flink SQL WITH 别名配置（前缀） |

更多 Fire 框架参数见 [properties.md](../properties.md#十二hudi--paimon-参数)。
