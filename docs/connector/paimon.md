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

# Paimon 读写

　　Apache Paimon（原 Flink Table Store）是面向流批一体的湖存储格式。Fire 框架对 Paimon 进行了集成封装：Spark 侧自动加载 Catalog 与 SQL Extensions；Flink 侧自动创建 Hive Metastore 类型的 Paimon Catalog，并支持通过 `@Paimon` 注解将表级参数映射到 Flink SQL 的 `datasource` 别名配置。

## 一、版本支持

Fire 通过 Maven Profile 适配多个 Paimon 版本，构建时需同时指定 **引擎 Profile** 与 **Paimon Profile**：

```shell
# 示例：Spark 3.3 + Paimon 1.2.0
mvn clean install -DskipTests -Pspark-3.3 -Ppaimon-1.2.0 -Pscala-2.12

# 示例：Flink 1.18 + Paimon 1.3.1
mvn clean install -DskipTests -Pflink-1.18 -Ppaimon-1.3.1 -Pscala-2.12
```

| Maven Profile | Paimon 版本 | 说明 |
| --- | --- | --- |
| `paimon-0.8` | 0.8.2 | 早期版本 |
| `paimon-0.9` | 0.9.0 | — |
| `paimon-1.0.1` | 1.0.1.4-SNAPSHOT | 开发预览 |
| `paimon-1.1.1` | 1.1.1 | — |
| `paimon-1.2.0` | 1.2.0 | 推荐生产环境 |
| `paimon-1.3.1` | 1.3.1 | 最新支持版本 |

**引擎适配说明：**

| 引擎 | 支持方式 | 父类 |
| --- | --- | --- |
| Spark Core / Streaming | 自动注册 Paimon Catalog，通过 SQL 读写 | `PaimonCore` / `PaimonStreaming` |
| Flink Streaming / SQL | 自动 `CREATE CATALOG paimon`，Flink SQL 读写 | `com.zto.fire.flink.sql.connector.paimon.PaimonStreaming` |

> Spark 3.3 Profile 在根 `pom.xml` 中内置了 `paimon-spark-common` 与 `paimon-spark-${spark.major.version}` 依赖；其他 Spark 版本构建时需自行确认 Paimon 与 Spark 版本的兼容性。

---

## 二、数据源配置

### 2.1 Spark：`paimon.properties`

继承 `PaimonCore` / `PaimonStreaming` 后，框架自动加载内置 **`paimon.properties`**，并依据 `@Hive` 配置将 Metastore URI 写入 Catalog：

```properties
# 内置 paimon.properties（框架自动加载）
spark.sql.catalog.paimon                =       org.apache.paimon.spark.SparkCatalog
spark.sql.extensions                    =       org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions
spark.sql.catalog.paimon.metastore      =       hive
spark.sql.catalog.paimon.warehouse      =       /user/hive/warehouse

# 运行时由 @Hive 自动注入
spark.sql.catalog.paimon.uri            =       thrift://your-hms:9083
```

### 2.2 Flink：`paimon.properties` 与 Catalog

Flink 任务继承 `PaimonStreaming` 后，在 `preProcess()` 阶段自动执行：

```sql
CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'metastore' = 'hive',
  'uri' = '<来自 hive.cluster 或 fire.hive.cluster.map 映射>'
)
```

可通过以下配置调整：

```properties
paimon.catalog.name                     =       paimon
hive.cluster                            =       fat
fire.hive.cluster.map.fat               =       thrift://your-hms:9083
```

框架内置 **`paimon.properties`** 还包含入湖任务推荐的 checkpoint / 内存调优参数（如 `flink.stream.checkpoint.timeout`、`taskmanager.memory.managed.fraction` 等），继承 Paimon 父类后自动生效。

### 2.3 基于 `@Paimon` 注解（Flink SQL）

`@Paimon` 的 **`datasource`** 参数与建表语句中 `'datasource' = 'xxx'` 保持一致，注解属性自动映射为 `flink.sql.with.{datasource}.{option}`：

```scala
@Paimon(
  datasource = "paimon_lake",
  bucket = 128,
  mergeEngine = "deduplicate",
  writeOnly = true,
  partitionTTL = "31d",
  value =
    """
      |snapshot.time-retained=3d
      |file.format=parquet
      |""")
```

等价于配置文件：

```properties
flink.sql.with.paimon_lake.bucket=128
flink.sql.with.paimon_lake.merge-engine=deduplicate
flink.sql.with.paimon_lake.write-only=true
flink.sql.with.paimon_lake.partition.expiration-time=31d
flink.sql.with.paimon_lake.snapshot.time-retained=3d
flink.sql.with.paimon_lake.file.format=parquet
```

`@Paimon2` ~ `@Paimon26` 支持多数据源场景。

---

## 三、Spark API 使用

### [3.1 Spark Core 离线查询](../fire-examples/spark-examples/src/main/scala/com/zto/fire/examples/spark/paimon/PaimonTest.scala)

```scala
@Hive("thrift://ip:9083")
object PaimonTest extends PaimonCore {

  override def process(): Unit = {
    sql("use paimon.paimon")
    sql("show tables").show
    sql("select * from paimon.paimon.paimon_table_name where ds=xxx").show
  }
}
```

**说明：**

- 继承 `PaimonCore`（或 `PaimonStreaming`）即可，无需手动注册 Catalog
- 三级命名空间：`paimon.{database}.{table}`，其中 `paimon` 为 Catalog 名（可通过 `spark.sql.catalog.paimon` 配置修改）
- 也可切换为 `extends PaimonStreaming`，在 Streaming 批次中访问 Paimon 表

### [3.2 Spark Streaming 批次访问](../fire-examples/spark-examples/src/main/scala/com/zto/fire/examples/spark/paimon/PaimonStreamingTest.scala)

```scala
@Hive("thrift://ip:9083")
@Streaming(interval = 60)
object PaimonStreamingTest extends PaimonStreaming {

  override def process(): Unit = {
    val dstream = this.fire.createRandomIntStream(1)
    dstream.foreachRDD { _ =>
      sql("use paimon.paimon")
      sql("show tables").show
      sql("select * from paimon.paimon.paimon_table_name where ds=xxx").show
    }
  }
}
```

### [3.3 血缘与 CTAS 示例](../fire-examples/spark-examples/src/main/scala/com/zto/fire/examples/spark/lineage/SparkCoreLineageTest.scala)

```scala
@Hive(value = "fat")
object SparkCoreLineageTest extends PaimonStreaming {

  override def process: Unit = {
    sql("drop table if exists tmp.paimon_xiaotiantong")
    val df = sql(
      """
        |create table tmp.paimon_xiaotiantong as
        |select * from paimon.paimon.paimon_xiaotiantong
        |""".stripMargin)
    df.show
    sql("select * from tmp.paimon_xiaotiantong limit 10").show
  }
}
```

Fire 会自动解析 Paimon 表的血缘信息，可配合 `LineageManager` 输出到 Kafka。

---

## 四、Flink API 使用

### [4.1 Flink SQL 建表与读写](../fire-examples/flink-examples/src/main/scala/com/zto/fire/examples/flink/lineage/FlinkSqlLineageTest.scala)

Flink 侧可直接使用原生 Paimon Connector 语法；若继承 `PaimonStreaming`，Catalog 会自动创建：

```scala
import com.zto.fire.flink.sql.connector.paimon.PaimonStreaming

@Hive("thrift://localhost:9083")
object MyPaimonJob extends PaimonStreaming {

  @Process
  def process: Unit = {
    usePaimonCatalog()  // 切换到 paimon catalog

    sql(
      """
        |CREATE TABLE t_paimon (
        |    user_id BIGINT, item_id BIGINT, behavior STRING,
        |    dt STRING, hh STRING,
        |    PRIMARY KEY (dt, hh, user_id) NOT ENFORCED
        |) PARTITIONED BY (dt, hh) WITH (
        |    'connector' = 'paimon',
        |    'warehouse' = 'hdfs:///tmp/test',
        |    'bucket' = '2',
        |    'bucket-key' = 'user_id'
        |)
        |""".stripMargin)

    sql("INSERT INTO t_paimon SELECT ... FROM source")
  }
}
```

配合 `@Paimon(datasource = "my_ds", bucket = 2, ...)` 时，建表 SQL 可简化为：

```sql
CREATE TABLE t_paimon (...) WITH (
  'connector' = 'paimon',
  'datasource' = 'my_ds'
)
```

敏感 path / warehouse 等参数放在 `flink.sql.with.my_ds.*` 配置中。

---

## 五、`@Paimon` 注解参数

| 注解属性 | 对应 Paimon 参数 | 说明 |
| --- | --- | --- |
| `datasource` | — | **必填**，与 SQL 中 `datasource` 一致 |
| `bucket` | `bucket` | 分桶数 |
| `mergeEngine` | `merge-engine` | 默认 `deduplicate` |
| `fileFormat` | `file.format` | 默认 `parquet` |
| `writeOnly` | `write-only` | 离线异步合并，生产建议 `true` |
| `partitionTTL` | `partition.expiration-time` | 分区过期，如 `31d` |
| `partitionFormat` | `partition.timestamp-formatter` | 如 `yyyyMMdd` |
| `snapshotTTL` | `snapshot.time-retained` | 快照保留时间 |
| `snapshotNumMin/Max` | `snapshot.num-retained.min/max` | 快照数量限制 |
| `tagOnSavepoint` | `sink.savepoint.auto-tag` | Savepoint 时自动打 Tag |
| `tagAutoCreate` | `tag.automatic-creation` | 自动创建 Tag |
| `tagTTL` | `tag.default-time-retained` | Tag 保留时间 |
| `compactionTrigger` | `num-sorted-run.compaction-trigger` | 触发 compaction 的 sorted run 数 |
| `mergeBufferSize` | `local-merge-buffer-size` | 本地合并缓冲区 |
| `writeBufferSize` | `write-buffer-size` | 写缓冲区大小 |
| `value` / `props` | 任意 Paimon 参数 | 透传原生配置 |

---

## 六、配置参数汇总

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| paimon.catalog.name | paimon | Flink Paimon Catalog 名称 |
| spark.sql.catalog.paimon | SparkCatalog 类名 | Spark Catalog 实现 |
| spark.sql.catalog.paimon.uri | — | Spark HMS URI（可由 @Hive 自动注入） |
| spark.sql.catalog.paimon.metastore | hive | Spark Catalog 类型 |
| spark.sql.catalog.paimon.warehouse | /user/hive/warehouse | 仓库路径 |
| flink.sql.with.{ds}.{option} | — | Flink SQL WITH 别名（@Paimon 映射目标） |

更多参数见 [properties.md](../properties.md#十二hudi--paimon-参数)。
