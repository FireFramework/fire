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

# fire框架参数

　　Fire 框架提供大量可配置参数，便于任务调优与运维集成。参数按用途分布在 **fire.properties**、**spark.properties**、**flink.properties** 及各 connector 文档中。

**配置约定：**

1. **自适应前缀**（`fire.conf.adaptive.prefix=true`，默认开启）：Spark 任务中 `kafka.brokers.name` 等价于 `spark.kafka.brokers.name`；Flink 任务中等价于 `flink.kafka.brokers.name`。Hive/HBase/JDBC 等 connector 参数同理。
2. **keyNum 后缀**：多数据源时在参数名末尾加数字，如 `db.jdbc.url2`、`fire.hbase.thread.num3`。
3. **前缀型参数**：以 `.` 结尾的键为前缀，可接任意后缀，如 `fire.kafka.cluster.map.test`、`fire.hbase.conf.hbase.rpc.timeout`。
4. **命令行覆盖**：`conf.overwrite.{key}=value` 优先级高于本地配置文件。

---

# 一、fire 框架核心参数

| 参数 | 默认值 | 含义 | 是否废弃 |
| --- | --- | --- | --- |
| fire.version | 1.0.0 | Fire 框架版本号 | 否 |
| driver.class.name | — | 任务主类名（框架自动设置） | 否 |
| fire.job.run.mode | auto | 任务运行模式 | 否 |
| fire.job.autoStart | true | 是否自动提交 job | 否 |
| fire.env.local | false | 是否 local 模式（影响配置文件加载策略） | 否 |
| fire.thread.pool.size | 5 | fire 内置线程池大小 | 否 |
| fire.thread.pool.schedule.size | 5 | fire 内置定时任务线程池大小 | 否 |
| fire.shared.threadPool.size | 5 | 共享线程池大小（如 foreachPartitionAsync） | 否 |
| fire.rest.enable | true | 是否启用内置 RESTful 服务 | 否 |
| fire.rest.filter.enable | true | REST 接口权限认证 | 否 |
| fire.rest.server.secret | — | REST 认证秘钥 | 否 |
| fire.rest.url | — | REST 服务访问地址 | 否 |
| fire.rest.url.hostname | false | REST 地址是否使用 hostname | 否 |
| fire.rest.url.show.enable | false | 是否在日志中打印 REST 地址 | 否 |
| fire.restful.max.thread | 5 | REST 服务最大线程数 | 否 |
| fire.restful.port.retry_num | 3 | REST 端口冲突重试次数 | 否 |
| fire.restful.port.retry_duration | 1000 | 端口重试间隔（ms） | 否 |
| fire.acc.enable | true | 累加器总开关 | 否 |
| fire.acc.log.enable | true | 日志累加器 | 否 |
| fire.acc.multi.counter.enable | true | 多值累加器 | 否 |
| fire.acc.multi.timer.enable | true | 时间维度累加器 | 否 |
| fire.acc.env.enable | true | env 累加器 | 否 |
| fire.acc.log.min.size | 500 | 日志累加器最小保留条数 | 否 |
| fire.acc.log.max.size | 1000 | 日志累加器最大保留条数 | 否 |
| fire.acc.env.min.size | 100 | env 累加器最小保留条数 | 否 |
| fire.acc.env.max.size | 500 | env 累加器最大保留条数 | 否 |
| fire.acc.timer.max.size | 1000 | timer 累加器最大记录数 | 否 |
| fire.acc.timer.max.hour | 12 | timer 清理 N 小时前的记录 | 否 |
| fire.acc.sync.max.size | 100 | 分布式累加器同步最大字符串数 | 否 |
| fire.log.enable | true | 埋点日志开关 | 否 |
| fire.log.sql.length | 50 | SQL 日志字符串长度限制 | 否 |
| fire.log.level.conf.{package} | — | 按包名设置日志级别（前缀） | 否 |
| fire.conf.show.enable | false | 是否打印非敏感配置信息 | 否 |
| fire.conf.print.blacklist | — | 配置打印黑名单，逗号分隔 | 否 |
| fire.conf.adaptive.prefix | true | 是否自动为配置加引擎前缀 | 否 |
| fire.conf.deploy.engine | — | 引擎配置同步实现类 | 否 |
| fire.conf.anno.manager.class | — | 注解配置管理器实现类 | 否 |
| fire.conf.annotation.enable | true | 是否启用注解配置 | 否 |
| fire.config.files | — | 额外配置文件列表（逗号分隔） | 否 |
| fire.config_center.enable | true | 是否启用配置中心 | 否 |
| fire.config_center.local.enable | false | 本地环境是否调用配置中心 | 否 |
| fire.config_center.app.id | — | 任务唯一 ID | 否 |
| fire.config_center.register.conf.secret | — | 配置中心接口秘钥 | 否 |
| fire.config_center.register.conf.prod.address | — | 配置中心生产地址 | 否 |
| fire.config_center.register.conf.test.address | — | 配置中心测试地址 | 否 |
| fire.config_center.register.conf.zdp.header.key | — | 配置中心请求 Header Key | 否 |
| fire.config_center.register.conf.zdp.header.value | — | 配置中心请求 Header Value | 否 |
| fire.user.common.conf | — | 用户公共配置文件（逗号分隔，优先级低于任务配置） | 否 |
| fire.task.schedule.enable | true | 是否启用 Quartz 定时调度 | 否 |
| fire.scheduler.blacklist | — | 定时任务方法名黑名单（逗号分隔） | 否 |
| fire.quartz.max.thread | 8 | Quartz 最大线程数 | 否 |
| fire.dynamic.conf.enable | true | 是否启用运行时动态配置 | 否 |
| fire.deploy_conf.enable | true | 是否将配置同步到 Executor/TM | 否 |
| fire.connector.shutdown_hook.enable | false | connector 是否注册 shutdown hook | 否 |
| fire.shutdown.auto.exit | false | shutdown 后是否 System.exit | 否 |
| fire.debug.shutdown.sleep | 5000 | JVM 退出前阻塞时间（ms） | 否 |
| fire.exception_bus.size | 1000 | 异常队列最大大小 | 否 |
| fire.print.limit | 1000000 | print 输出条数上限 | 否 |
| fire.distribute.execute.enable | true | 是否启用分布式执行 | 否 |
| fire.distribute.execute.class | — | 分布式执行器实现类 | 否 |
| fire.distribute.sync.enable | true | Flink 分布式数据同步开关 | 否 |
| fire.error.tolerance.level | NONE | 容错级别（NONE/TASK/STAGE/JOB/Container/Master） | 否 |
| fire.error.tolerance.threshold | 0 | 容错阈值 | 否 |
| fire.container.monitor.enable | true | YARN 容器内存监控 | 否 |
| fire.container.monitor.interval | 10000 | 容器监控周期（ms） | 否 |
| fire.container.monitor.pmem.ratio | 1.8 | 物理内存上限比例 | 否 |
| fire.container.monitor.vmem.ratio | 2.0 | 虚拟内存上限比例 | 否 |
| fire.encrypt.private.key.prod | — | 生产环境 RSA 私钥 | 否 |
| fire.encrypt.private.key.test | — | 测试环境 RSA 私钥 | 否 |
| fire.consumer.offsets | — | 指定消费位点 JSON | 否 |
| fire.consumer.offset.export.enable | false | 是否将 offset 导出到 MQ | 否 |
| fire.consumer.offset.export.mq.url | — | offset 导出 Kafka 地址 | 否 |
| fire.consumer.offset.export.mq.topic | — | offset 导出 topic | 否 |
| fire.debug.class.code.resource | — | 类加载路径调试列表 | 否 |
| fire.sql_conf.{key} | — | SQL set 配置（前缀，如 hive.exec.dynamic.partition） | 否 |
| sql.conf.{key} | — | SQL set 配置（前缀，同上） | 否 |
| hive.conf.{key} | — | Hive set 配置（前缀） | 否 |
| conf.overwrite.{key} | — | 命令行覆盖参数（前缀，最高优先级之一） | 否 |

---

# 二、血缘参数（fire.lineage.*）

> 原 `fire.buried_point.*` 已废弃，请迁移至下表对应项。

| 参数 | 默认值 | 含义 | 是否废弃 |
| --- | --- | --- | --- |
| fire.lineage.enable | true | 实时血缘采集开关 | 否 |
| fire.lineage.listener.enable | true | Spark Listener 血缘解析 | 否 |
| fire.lineage.column.enable | false | 是否采集字段级血缘 | 否 |
| fire.lineage.collect_sql.enable | true | 是否采集原始 SQL | 否 |
| fire.lineage.debug.enable | false | 血缘 debug 模式 | 否 |
| fire.lineage.debug.print | false | 打印血缘 JSON 到 stdout | 否 |
| fire.lineage.max.size | 500 | 血缘队列最大大小 | 否 |
| fire.lineage.run.initialDelay | 5 | 定时解析初始延迟（s） | 否 |
| fire.lineage.run.period | 5 | 定时解析频率（s） | 否 |
| fire.lineage.run.count | 36000 | 异步解析线程执行次数 | 否 |
| fire.lineage.datasource.map.{dbType} | — | JDBC URL 端口识别映射（前缀，如 tidb=4000） | 否 |
| fire.lineage.send.mq.enable | false | 是否发送血缘到 MQ | 否 |
| fire.lineage.send.mq.url | — | 血缘 MQ 地址 | 否 |
| fire.lineage.send.mq.topic | — | 血缘 MQ topic | 否 |
| fire.lineage.active.stage.threshold | 2 | 活跃 stage 触发阈值 | 否 |
| fire.lineage.distribute.collect.period | 120 | 分布式采集频率（s） | 否 |
| fire.lineage.shutdown.sleep | 5 | 任务退出前等待血缘解析（s） | 否 |
| fire.buried_point.datasource.* | — | **已废弃**，请改用 fire.lineage.* | 是 |

---

# 三、诊断与分析参数

## 3.1 Arthas 性能分析（fire.analysis.arthas.*）

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| fire.analysis.arthas.enable | false | 启用 Arthas 性能分析 |
| fire.analysis.arthas.container.enable | false | 在 container 端启动 Arthas |
| fire.analysis.arthas.tunnel_server.url | — | Arthas tunnel 服务地址 |
| fire.analysis.arthas.launcher | — | Arthas 启动器类 |
| fire.analysis.arthas.conf.{key} | — | Arthas 参数（前缀） |

## 3.2 异常堆栈采集（fire.analysis.log.exception.*）

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| fire.analysis.log.exception.stack.enable | false | 异常堆栈采集开关 |
| fire.analysis.log.exception.send.maxRetires | 10 | 发送 MQ 最大重试次数 |
| fire.analysis.log.exception.send.timeout | 3000 | 发送超时（ms） |
| fire.analysis.log.exception.send.mq.url | — | 异常 MQ 地址 |
| fire.analysis.log.exception.send.mq.topic | — | 异常 MQ topic |
| fire.analysis.log.exception.send.mq.message.max.size | 1048576 | 消息体大小阈值（字节） |

## 3.3 代码 Trace 与标准化（fire.trace.*）

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| fire.trace.launcher | — | Trace 启动器类 |
| fire.trace.codeTrace.enable | false | 代码增强 Trace |
| fire.trace.codeTrace.class | — | 追踪目标类.方法（* 表示全部方法） |
| fire.trace.codeTrace.thresholdMs | 10 | 最小耗时阈值（ms） |
| fire.trace.codeStandard.enable | true | 代码标准化分析 |
| fire.trace.codeStandard.api | — | 原生/Fire API 映射（Base64 JSON） |
| fire.trace.codeStandard.durationMin | 10 | 分析时长（分钟） |
| fire.trace.codeStandard.autoExit | true | 发现违规 API 是否退出 |
| fire.trace.codeStandard.stackScanDepth | 28 | 调用栈扫描深度 |
| fire.trace.codeStandard.run.initialDelay | 30 | 分布式采集初始延迟（s） |
| fire.trace.codeStandard.run.period | 60 | 分布式采集周期（s） |
| fire.trace.codeStandard.run.count | 10 | 最大采集次数 |
| fire.trace.codeStandard.send.mq.url | — | 结果发送 Kafka 地址 |
| fire.trace.codeStandard.send.mq.topic | — | 结果发送 topic |

---

# 四、JDBC 参数

## 4.1 连接池（db.jdbc.*）

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| db.jdbc.url | — | JDBC 连接 URL |
| db.jdbc.url.map.{alias} | — | JDBC URL 别名映射（前缀） |
| db.jdbc.driver | — | JDBC 驱动类 |
| db.jdbc.user | — | 数据库用户名 |
| db.jdbc.password | — | 数据库密码 |
| db.jdbc.isolation.level | READ_UNCOMMITTED | 事务隔离级别 |
| db.jdbc.maxPoolSize | 5 | 连接池最大连接数 |
| db.jdbc.minPoolSize | 1 | 连接池最小连接数 |
| db.jdbc.initialPoolSize | 1 | 连接池初始连接数 |
| db.jdbc.acquireIncrement | 1 | 连接不足时自增量 |
| db.jdbc.maxIdleTime | 30 | 空闲连接释放时间（s） |
| db.jdbc.connection.timeout | 60 | 连接超时（s） |
| db.jdbc.batch.size | 1000 | 批量操作记录数 |
| db.jdbc.flushInterval | 0 | flush 间隔（ms，Flink Sink） |
| db.jdbc.max.retry | 3 | 失败最大重试次数 |
| db.query.use.label | true | 查询结果是否使用别名映射 JavaBean |
| db.c3p0.conf.{key} | — | c3p0 连接池配置（前缀，按 keyNum） |
| db.c3p0.common.conf.{key} | — | c3p0 公共配置（前缀） |

## 4.2 Fire JDBC 扩展（fire.jdbc.*）

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| fire.jdbc.storage.level | memory_and_disk_ser | JDBC 结果集缓存策略 |
| fire.jdbc.query.partitions | 10 | 查询后 repartition 分区数（按 keyNum 默认 -1） |
| fire.jdbc.thread.num | 2 | JDBC 多线程并发写/查线程数 |

---

# 五、HBase 参数

| 参数 | 默认值 | 含义 | 是否废弃 |
| --- | --- | --- | --- |
| hbase.cluster | — | HBase 集群别名或 ZK 地址 | 否 |
| hbase.column.family | info | 默认列族 | 否 |
| hbase.max.retry | 3 | 操作失败最大重试（Flink Sink） | 否 |
| hbase.user | — | HBase 用户名 | 否 |
| hbase.durability | — | WAL durability | 否 |
| fire.hbase.batch.size | 10000 | 单批次读写记录数 | 否 |
| fire.hbase.thread.num | 2 | HBase Java API 多线程并发数 | 否 |
| fire.hbase.storage.level | memory_and_disk_ser | scan 结果缓存策略 | 否 |
| fire.hbase.scan.partitions | -1 | scan 后 repartition 分区数（-1 不生效） | 否 |
| fire.hbase.scan.repartitions | 1200 | scan 分区数（已废弃，fallback） | 是 |
| fire.hbase.cluster.map.{alias} | — | HBase 集群别名映射（前缀） | 否 |
| fire.hbase.table.exists.cache.enable | true | 表存在判断缓存 | 否 |
| fire.hbase.table.exists.cache.reload.enable | true | 表列表缓存定时刷新 | 否 |
| fire.hbase.table.exists.cache.initialDelay | 60 | 缓存刷新初始延迟（s） | 否 |
| fire.hbase.table.exists.cache.period | 600 | 缓存刷新周期（s） | 否 |
| fire.hbase.conf.{hbaseKey} | — | HBase Java API 配置（前缀） | 否 |
| spark.hbase.blockcache.enable | true | Spark HBase 连接器 Block Cache | 否 |
| spark.hbase.cacheSize | 1000 | Spark HBase 缓存大小 | 否 |
| spark.hbase.batchNum | 1000 | Spark HBase 批次数量 | 否 |
| spark.hbase.bulkGetSize | 1000 | Spark HBase bulk get 大小 | 否 |

---

# 六、Hive 参数

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| hive.cluster | — | Hive 集群标识（支持别名） |
| hive.version | 1.1.0 | Hive 版本号 |
| hive.catalog.name | hive | Hive catalog 名称 |
| fire.hive.default.database.name | tmp | 默认 Hive 库名 |
| fire.hive.table.default.partition.name | ds | 默认分区字段名 |
| fire.hive.metastore.url.random.enable | true | Metastore URL 随机选择 |
| fire.hive.cluster.map.{alias} | — | Metastore 地址别名映射（前缀） |
| fire.hive.site.path.map.{alias} | — | hive-site.xml 路径映射（前缀） |

> Spark/Flink 任务中通过 `spark.hive.cluster` / `flink.hive.cluster` 引用（自适应前缀），默认库名/分区字段使用 `fire.hive.default.database.name` 和 `fire.hive.table.default.partition.name`。

---

# 七、Kafka 参数

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| fire.kafka.cluster.map.{alias} | — | Kafka 集群别名映射（前缀） |
| kafka.brokers.name | — | Broker 地址或别名 |
| kafka.topics | — | Topic 列表（逗号分隔） |
| kafka.group.id | — | Consumer group id（空则取类名） |
| kafka.starting.offsets | — | 起始消费位点 |
| kafka.ending.offsets | — | 结束消费位点 |
| kafka.enable.auto.commit | false | 是否自动 commit offset |
| kafka.failOnDataLoss | true | 数据丢失时是否失败 |
| kafka.session.timeout.ms | 300000 | Session 超时（ms） |
| kafka.request.timeout.ms | 400000 | Request 超时（ms） |
| kafka.max.poll.interval.ms | 600000 | Poll 间隔（ms） |
| kafka.CommitOffsetsOnCheckpoints | true | Checkpoint 时记录 offset（Flink） |
| kafka.StartFromTimestamp | 0 | 从时间戳开始消费 |
| kafka.StartFromGroupOffsets | false | 从 group 上次位点开始 |
| kafka.force.overwrite.stateOffset.enable | false | 强制覆盖状态 offset |
| kafka.force.autoCommit.enable | false | 强制周期性 commit |
| kafka.force.autoCommit.Interval | 30000 | 强制 commit 间隔（ms） |
| kafka.sink.batch | -1 | Kafka Sink 批次大小 |
| kafka.sink.flashInterval | -1 | Kafka Sink flush 间隔（ms） |
| kafka.conf.{clientKey} | — | Kafka client 原生配置（前缀） |

---

# 八、RocketMQ 参数

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| fire.rocket.cluster.map.{alias} | — | RocketMQ 集群别名映射（前缀） |
| rocket.brokers.name | — | NameServer 地址或别名 |
| rocket.topics | — | Topic 列表 |
| rocket.group.id | — | Consumer group id |
| rocket.starting.offsets | — | 起始消费位点 |
| rocket.failOnDataLoss | true | 数据丢失是否失败 |
| rocket.forceSpecial | false | 强制特殊处理 |
| rocket.enable.auto.commit | false | 自动 commit |
| rocket.consumer.tag | — | 订阅 tag |
| rocket.pull.max.speed.per.partition | — | 每 partition 拉取速率 |
| rocket.consumer.instance | — | 消费者实例 ID |
| rocket.sink.parallelism | -1 | Sink 并行度 |
| rocket.sink.batch | -1 | Sink 批次 |
| rocket.sink.flashInterval | -1 | Sink flush 间隔（ms） |
| rocket.force.overwrite.stateOffset.enable | false | 覆盖状态 offset |
| rocket.StartFromTimestamp | -1 | 从时间戳消费 |
| rocket.logger.debug.enable | false | 详细日志 |
| rocket.conf.{clientKey} | — | RocketMQ client 配置（前缀） |

---

# 九、HDFS 参数

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| hdfs.ha.enable | true | 是否启用 HDFS HA |
| hdfs.ha.conf.{cluster}.{hdfsKey} | — | HA 配置（前缀+集群名） |
| hdfs.user | hadoop | HDFS 访问用户 |
| hdfs.url | — | HDFS URL 前缀 |
| hdfs.conf.{key} | — | 通用 HDFS 配置（前缀） |

Spark 自适应等价：`spark.hdfs.ha.enable`、`spark.hdfs.ha.conf.{cluster}.*`。

---

# 十、Spark 引擎参数

| 参数 | 默认值 | 含义 | 是否废弃 |
| --- | --- | --- | --- |
| spark.appName | — | 应用名称（空则取类名） | 否 |
| spark.local.cores | * | local 模式 core 数 | 否 |
| spark.log.level | info | Spark 日志级别 | 否 |
| spark.saveMode | Append | 默认 saveMode | 否 |
| spark.parallelism | 200 | 默认并行度 | 否 |
| spark.chkpoint.dir | hdfs://nameservice1/user/spark/ckpoint/ | Checkpoint 目录 | 否 |
| spark.fire.sql.extensions.enable | true | SQL 扩展（血缘等） | 否 |
| spark.fire.stage.maxFailures | -1 | Stage 失败退出阈值（-1 不限制） | 否 |
| spark.fire.scheduler.blacklist | jvmMonitor | 引擎级定时任务黑名单 | 否 |
| spark.streaming.batch.duration | -1 | Streaming 批次时间（ms） | 否 |
| spark.streaming.remember | -1 | Streaming remember 时间（ms） | 否 |
| spark.redaction.regex | — | 日志脱敏正则 | 否 |
| spark.fire.conf.deploy.engine | — | Spark 配置同步实现类 | 否 |
| spark.sql.queryExecutionListeners | — | SQL 血缘 Listener 类 | 否 |
| spark.datasource.format | — | DataSource format | 否 |
| spark.datasource.saveMode | Append | DataSource saveMode | 否 |
| spark.datasource.saveParam | — | write.save() 参数 | 否 |
| spark.datasource.isSaveTable | — | save 还是 saveAsTable | 否 |
| spark.datasource.loadParam | — | read.load() 参数 | 否 |
| spark.datasource.options.{key} | — | DataSource options（前缀） | 否 |
| spark.impala.connection.url | — | Impala JDBC URL | 否 |
| spark.impala.jdbc.driver.class.name | org.apache.hive.jdbc.HiveDriver | Impala 驱动 | 否 |
| spark.kafka.* | 见第七节 | Kafka 配置（自适应前缀） | 否 |
| spark.hive.cluster | 见第六节 | Hive 集群（= hive.cluster） | 否 |
| spark.hbase.cluster | 见第五节 | HBase 集群（= hbase.cluster） | 否 |
| spark.rocket.* | 见第八节 | RocketMQ 配置（自适应前缀） | 否 |
| spark.hdfs.ha.* | 见第九节 | HDFS HA 配置（自适应前缀） | 否 |

---

# 十一、Flink 引擎参数

| 参数 | 默认值 | 含义 | 是否废弃 |
| --- | --- | --- | --- |
| flink.appName | — | 应用名称（空则取类名，properties 约定） | 否 |
| flink.log.level | WARN | 日志级别（properties 约定，实际可用 fire.log.level.conf.*） | 否 |
| flink.auto.generate.uid.enable | true | 自动生成算子 UID | 否 |
| flink.auto.type.registration.enable | true | 自动类型注册 | 否 |
| flink.force.avro.enable | false | 强制 Avro 序列化 | 否 |
| flink.force.kryo.enable | false | 强制 Kryo 序列化 | 否 |
| flink.generic.types.enable | true | 泛型类型支持 | 否 |
| flink.object.reuse.enable | false | 对象复用 | 否 |
| flink.auto.watermark.interval | -1 | Watermark 间隔（ms） | 否 |
| flink.closure.cleaner.level | — | Closure cleaner 级别 | 否 |
| flink.default.input.dependency.constraint | — | 输入依赖约束 | 否 |
| flink.execution.mode | — | 执行模式 | 否 |
| flink.runtime.mode | STREAMING | 运行时模式（BATCH/STREAMING） | 否 |
| flink.latency.tracking.interval | -1 | 延迟追踪间隔 | 否 |
| flink.max.parallelism | 1024 | 最大并行度 | 否 |
| flink.default.parallelism | -1 | 默认并行度 | 否 |
| flink.task.cancellation.interval | -1 | 任务取消间隔 | 否 |
| flink.task.cancellation.timeout.millis | -1 | 任务取消超时 | 否 |
| flink.use.snapshot.compaction | false | Snapshot 压缩 | 否 |
| flink.stream.buffer.timeout.millis | -1 | Stream buffer 超时 | 否 |
| flink.stream.number.execution.retries | -1 | 执行重试次数 | 否 |
| flink.stream.time.characteristic | — | 时间特性 | 否 |
| flink.env.operatorChaining.enable | true | 算子链合并 | 否 |
| flink.state.ttl.days | 31 | Keyed State TTL（天） | 否 |
| flink.state.clean.hdfs.url | — | 状态清理 HDFS 路径 | 否 |
| flink.stream.checkpoint.interval | -1 | Checkpoint 间隔（ms，-1 关闭） | 否 |
| flink.stream.checkpoint.mode | EXACTLY_ONCE | Checkpoint 模式 | 否 |
| flink.stream.checkpoint.timeout | 600000 | Checkpoint 超时（ms） | 否 |
| flink.stream.checkpoint.max.concurrent | 1 | 最大并发 checkpoint | 否 |
| flink.stream.checkpoint.min.pause.between | -1 | 两次 checkpoint 最小间隔 | 否 |
| flink.stream.checkpoint.prefer.recovery | false | 优先恢复最近 checkpoint | 否 |
| flink.stream.checkpoint.tolerable.failure.number | 0 | 可容忍失败次数 | 否 |
| flink.stream.checkpoint.externalized | RETAIN_ON_CANCELLATION | Cancel 时保留 checkpoint | 否 |
| flink.stream.checkpoint.unaligned.enable | true | 非对齐 checkpoint | 否 |
| flink.sql.log.enable | false | 是否打印组装后的 SQL | 否 |
| flink.sql.default.catalog.name | default_catalog | 默认 catalog | 否 |
| flink.sql.conf.pipeline.jars | — | UDF jar 路径 | 否 |
| flink.sql.udf.fireUdf.enable | true | 启用 Fire UDF 自动注册 | 否 |
| flink.sql.udf.conf.{funcName} | — | UDF 函数名→类名映射（前缀） | 否 |
| flink.sql.with.{ds}.{option} | — | SQL WITH 表达式配置（前缀） | 否 |
| flink.sql_with.replaceMode.enable | true | 强制替换 SQL 中已有 WITH | 否 |
| flink.sql.useStatementSet | true | 自动加入 StatementSet | 否 |
| flink.fire.conf.deploy.engine | — | Flink 配置同步实现类 | 否 |
| flink.kafka.* | 见第七节 | Kafka 配置（自适应前缀） | 否 |
| flink.hive.cluster / flink.hive.version / flink.hive.catalog.name | 见第六节 | Hive 配置（自适应前缀） | 否 |
| flink.hbase.cluster / flink.hbase.batch.size | 见第五节 | HBase 配置（自适应前缀） | 否 |

---

# 十二、Hudi / Paimon 参数

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| hudi.options.{key} | — | Spark 写 Hudi options（前缀） |
| paimon.catalog.name | paimon | Paimon catalog 名称（Flink） |
| spark.sql.catalog.paimon.uri | — | Spark Paimon catalog URI |

---

# 十三、多个数据源读写

Fire 框架支持同一任务读写多个数据源，通过 **keyNum** 区分。配置时在参数名末尾加数字，API 中通过 `keyNum` 参数指定（始终放在参数列表最后）。

```scala
// JDBC
this.fire.jdbcUpdate(sql, params, keyNum = 2)

// HBase
studentRDD.hbasePutRDD(tableName, keyNum = 3)

// Kafka（Flink）
flink.kafka.brokers.name2=alias_or_url
```

---

# 十四、@Jdbc 注解参数

```java
String url();                          // JDBC URL
String driver() default "";            // 驱动类，可自动推断
String username();                     // 用户名
String password() default "";          // 密码
String isolationLevel() default "";    // 事务隔离级别
int maxPoolSize() default -1;          // 连接池最大连接数
int minPoolSize() default -1;          // 连接池最小连接数
int initialPoolSize() default -1;      // 初始连接数
int acquireIncrement() default -1;     // 连接池增量
int maxIdleTime() default -1;          // 最大空闲时间
int batchSize() default -1;            // 批次大小
long flushInterval() default -1;       // flush 间隔（ms，Flink）
int maxRetries() default -1;           // 最大重试次数（Flink）
String storageLevel() default "";      // 结果集缓存级别（Spark）
int queryPartitions() default -1;      // 查询后分区数（Spark）
int logSqlLength() default -1;         // SQL 日志长度
String[] config() default {};          // c3p0 参数 key=value
```

---

# 十五、@HBase 注解参数

详见 [hbase.md](connector/hbase.md) 第六节 `@HBase` 注解说明。
