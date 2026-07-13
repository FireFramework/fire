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

# JDBC读写

　　实时任务开发中，对jdbc读写的需求很高。为了简化jdbc开发步骤，fire框架对jdbc操作做了进一步封装，将许多常见操作简化成一行代码。其中 Java API 还提供**多线程并发读写**能力（批量写方法带 `Async` 后缀，查询方法带 `Async` 后缀），适用于中等数据量的高吞吐场景。另外，fire框架支持在同一个任务中对任意多个数据源进行读写。

### 一、数据源配置

#### 1.1 基于注解

```scala
@Jdbc(url = "jdbc:derby:memory:fire;create=true", username = "fire", password = "fire")
@Jdbc3(url = "jdbc:derby:memory:fire;create=true", username = "fire", maxPoolSize=3, config=Array[String]("c3p0.key=value"))
```

#### 1.2 基于配置文件

　　数据源包括jdbc的url、driver、username与password等重要信息，建议将这些配置放到commons.properties中，避免每个任务单独配置。fire框架内置了c3p0数据库连接池，在分布式场景下，限制每个container默认最多3个connection，避免申请过多资源时申请太多的数据库连接。

```properties
db.jdbc.url                  =       jdbc:derby:memory:fire;create=true
db.jdbc.driver               =       org.apache.derby.jdbc.EmbeddedDriver
db.jdbc.maxPoolSize          =       3
db.jdbc.user                 =       fire
db.jdbc.password             =       fire

# 如果需要多个数据源，则可在每项配置的结尾添加对应的keyNum作为区分
db.jdbc.url2                 =       jdbc:mysql://mysql:3306/fire
db.jdbc.driver2              =       com.mysql.jdbc.Driver
db.jdbc.user2                =       fire
db.jdbc.password2            =       fire
```

### 二、API使用

#### [2.1 Spark 同步 API](../fire-examples/spark-examples/src/main/scala/com/zto/fire/examples/spark/jdbc/JdbcTest.scala)

```scala
/**
   * 使用jdbc方式对关系型数据库进行增删改操作
   */
def testJdbcUpdate: Unit = {
    val timestamp = DateFormatUtils.formatCurrentDateTime()
    // 执行insert操作
    val insertSql = s"INSERT INTO $tableName (name, age, createTime, length, sex) VALUES (?, ?, ?, ?, ?)"
    this.fire.jdbcUpdate(insertSql, Seq("admin", 12, timestamp, 10.0, 1))
    // 更新配置文件中指定的第二个关系型数据库
    this.fire.jdbcUpdate(insertSql, Seq("admin", 12, timestamp, 10.0, 1), keyNum = 2)

    // 执行更新操作
    val updateSql = s"UPDATE $tableName SET name=? WHERE id=?"
    this.fire.jdbcUpdate(updateSql, Seq("root", 1))

    // 执行批量操作
    val batchSql = s"INSERT INTO $tableName (name, age, createTime, length, sex) VALUES (?, ?, ?, ?, ?)"

    this.fire.jdbcBatchUpdate(batchSql, Seq(Seq("spark1", 21, timestamp, 100.123, 1),
                                            Seq("flink2", 22, timestamp, 12.236, 0),
                                            Seq("flink3", 22, timestamp, 12.236, 0),
                                            Seq("flink4", 22, timestamp, 12.236, 0),
                                            Seq("flink5", 27, timestamp, 17.236, 0)))

    // 执行批量更新
    this.fire.jdbcBatchUpdate(s"update $tableName set sex=? where id=?", Seq(Seq(1, 1), Seq(2, 2), Seq(3, 3), Seq(4, 4), Seq(5, 5), Seq(6, 6)))

    // 方式一：通过this.fire方式执行delete操作
    val sql = s"DELETE FROM $tableName WHERE id=?"
    this.fire.jdbcUpdate(sql, Seq(2))
    // 方式二：通过JdbcConnector.executeUpdate

    // 同一个事务
    /*val connection = this.jdbc.getConnection()
    this.fire.jdbcBatchUpdate("insert", connection = connection, commit = false, closeConnection = false)
    this.fire.jdbcBatchUpdate("delete", connection = connection, commit = false, closeConnection = false)
    this.fire.jdbcBatchUpdate("update", connection = connection, commit = true, closeConnection = true)*/
}

  /**
   * 将DataFrame数据写入到关系型数据库中
   */
  def testDataFrameSave: Unit = {
    val df = this.fire.createDataFrame(Student.newStudentList(), classOf[Student])

    val insertSql = s"INSERT INTO spark_test(name, age, createTime, length, sex) VALUES (?, ?, ?, ?, ?)"
    // 指定部分DataFrame列名作为参数，顺序要对应sql中问号占位符的顺序，batch用于指定批次大小，默认取spark.db.jdbc.batch.size配置的值
    df.jdbcBatchUpdate(insertSql, Seq("name", "age", "createTime", "length", "sex"), batch = 100)

    df.createOrReplaceTempViewCache("student")
    val sqlDF = this.fire.sql("select name, age, createTime from student where id>=1").repartition(1)
    // 若不指定字段，则默认传入当前DataFrame所有列，且列的顺序与sql中问号占位符顺序一致
    sqlDF.jdbcBatchUpdate("insert into spark_test(name, age, createTime) values(?, ?, ?)")
    // 等同以上方式
    // this.fire.jdbcBatchUpdateDF(sqlDF, "insert into spark_test(name, age, createTime) values(?, ?, ?)")
  }
```

#### [2.2 Spark 多线程 API](../fire-examples/spark-examples/src/main/scala/com/zto/fire/examples/spark/jdbc/JdbcTest.scala)

　　Fire 框架在 `JdbcConnector` 之上封装了多线程批量写和并发查询能力。多线程 API 与同步 API 相互独立，方法名带 **`Async`** 后缀；同步 API 签名不变。新增参数 **`threadNum`** 表示并发线程数，**`keyNum` 始终放在参数列表最后**。

　　实际并发度受连接池大小限制，不会超过 `db.jdbc.maxPoolSize` 与待处理数据分组数中的较小值。

```properties
# 全局默认 JDBC 并发线程数（默认 2）
fire.jdbc.thread.num=3
# 指定 keyNum=2 数据源的并发线程数
fire.jdbc.thread.num2=4
```

##### Driver 端批量写入

　　适用于数据量不大、已在 Driver 端聚合为参数列表的场景。数据按 `threadNum` 分组后，多线程并发执行 `updateBatch`。

```scala
def testJdbcUpdateAsync: Unit = {
  // 准备数据
  val stuRDD = this.fire.createRDD(1 to 1000, 2)
    .map(index => new Student(index, s"name-$index", index, java.math.BigDecimal.valueOf(index), true, DateFormatUtils.formatCurrentDateTime()))
  // 将 RDD 转为参数列表（需在 Driver 端 collect）
  val paramList = stuRDD.map(t => Seq(t.getName, t.getAge, t.getCreateTime, t.getLength, t.getSex)).collect().toSeq

  val insertSql = s"INSERT INTO $tableName (name, age, createTime, length, sex) VALUES (?, ?, ?, ?, ?)"
  // 参数顺序：sql、参数列表、threadNum、keyNum（可选）
  this.fire.jdbcUpdateBatchAsync(insertSql, paramList, threadNum = 3)
}
```

##### DataFrame 分布式写入

　　适用于 DataFrame 数据已在 Executor 上分布的场景。框架在每个 **partition 内部**按 `threadNum` 分组并发写入，各分组为独立事务，**不保证全局事务原子性**。写入前需确保数据已去重或对乱序无感。

```scala
def testJdbcUpdateAsync: Unit = {
  val df = this.fire.createDataFrame(stuRDD, classOf[Student])
  val insertSql2 = s"INSERT INTO spark_test2(name, age, createTime, length, sex) VALUES (?, ?, ?, ?, ?)"

  // 方式一：DataFrame 扩展方法
  df.jdbcUpdateBatchAsync(insertSql2, Seq("name", "age", "createTime", "length", "sex"), threadNum = 3)

  // 方式二：通过 fire 入口
  this.fire.jdbcUpdateBatchDFAsync(df, insertSql2, Seq("name", "age", "createTime", "length", "sex"), threadNum = 3)
}
```

##### 多线程查询

　　将多组查询参数拆分后并发执行，结果合并返回。适用于 IN 查询拆分、多租户批量查等场景。

```scala
// 以 DataFrame 方式返回（多组结果 union）
val df = this.fire.jdbcQueryDFAsync(
  "select * from spark_test where id = ?",
  Seq(Seq(1), Seq(2), Seq(3)),
  threadNum = 3
)
df.show()

// 以 RDD[Row] 方式返回
val rdd = this.fire.jdbcQueryRDDAsync(
  "select * from spark_test where id = ?",
  Seq(Seq(1), Seq(2), Seq(3)),
  threadNum = 3
)

// 自定义 ResultSet 处理逻辑
val results = this.fire.jdbcQueryAsync(sql, paramsList, threadNum = 3) { rs =>
  // 处理结果集，返回自定义类型
  rs.getInt(1)
}
```

##### 自定义 Partition 内多线程处理

　　若需在 partition 内自行组织 JDBC 逻辑，可使用 `foreachPartitionAsync`，框架自动将 partition 数据切分为 `threadNum` 份并行处理：

```scala
df.foreachPartitionAsync { rows =>
  // rows 为切分后的子集合，在此执行自定义 JDBC 操作
  val paramsList = rows.map(row => Seq(row.getAs[String]("name"), row.getAs[Int]("age"))).toSeq
  JdbcConnector.updateBatch(sql, paramsList)
}(threadNum = 3)
```

##### 完整调用流程参考

```scala
override def process: Unit = {
  // 多线程批量写入
  this.testJdbcUpdateAsync

  // 流式场景中在 foreachRDD 内调用
  dstream.foreachRDD { rdd =>
    this.testJdbcUpdateAsync
    this.fire.jdbcUpdate(s"delete from $tableName")
  }
}
```

　　**Spark 使用建议**：

- Driver 端 `jdbcUpdateBatchAsync` 需先将数据 `collect` 到 Driver，大数据量场景优先用 `df.jdbcUpdateBatchAsync`。
- `threadNum` 不宜超过连接池 `maxPoolSize`，否则多余线程会空闲等待。
- 并发写入各分组独立提交，对事务一致性有要求的场景请使用同步 API 或手动管理 `Connection`。

#### [2.3 Flink 同步 API](../fire-examples/flink-examples/src/main/scala/com/zto/fire/examples/flink/connector/jdbc/JdbcSinkTest.scala)

　　Flink 任务中 JDBC 写入通过 Sink 组件实现，同步 API 底层使用 `JdbcSink`，每次 flush 单线程批量写入。查询和增删改也可在 `process()` 中直接调用 `fire.jdbcQueryList` / `fire.jdbcUpdate`。

```scala
@Streaming(30)
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")
@Jdbc(url = "jdbc:mysql://mysql-server:3306/fire", username = "root", password = "root")
object JdbcSinkTest extends FlinkStreaming {

  @Process
  def kafkaSource: Unit = {
    // 执行单个查询，结果集直接封装到Student类的对象中，该api自动从指定的keyNum获取对应的数据源信息
    val students = this.fire.jdbcQueryList[Student]("select * from spark_test where age>?", Seq(1))
    println("总计：" + students.length)

    // 执行update、delete、insert、replace、merge等语句
    this.fire.jdbcUpdate("delete from spark_test where age>?", Seq(10), keyNum = 1)

    val dstream = this.fire.createKafkaDirectStream().map(t => JSONUtils.parseObject[Student](t))
    val sql =
      s"""
         |insert into spark_test(name, age, createTime) values(?, ?, '${DateFormatUtils.formatCurrentDateTime()}')
         |ON DUPLICATE KEY UPDATE age=18
         |""".stripMargin
    // 1. 将数据实时写入到@Jdbc指定的数据源，无需指定driverclass
    // 2. sinkJdbc只需指定sql语句即可，fire会自动推断sql中占位符与JavaBean中成员变量的对应关系，并自动设置到PreparedStatement中
    // 3. 支持update、delete、replace、merge、insert等语句
    // 4. 支持自动将下划线命名的字段与JavaBean中驼峰式命名的成员变量自动映射
    // 5. 如果是将数据写入其他数据源，可通过keyNum=xxx指定：
    //    dstream.sinkJdbc(sql, keyNum=3)表示将数据写入@Jdbc3所配置的数据源中
    dstream.sinkJdbc(sql)

    // sinkJdbcExactlyOnce支持仅一次的语义，默认支持mysql，如果是Oracle或PostgreSQL，可通过参数指定：
    // dstream.sinkJdbcExactlyOnce(sql, dbType = Datasource.ORACLE, keyNum=2)
    // Flink1.12不支持该API
    dstream.sinkJdbcExactlyOnce(sql, keyNum = 2)  }
}
```

　　基于 `DataStream` / `Table` 的批量 Sink 写法（同步）：

```scala
// DataStream：指定 JavaBean 字段与 SQL 占位符的映射
stream.jdbcBatchUpdate(sql, fields, keyNum = 6).setParallelism(3)

// DataStream：自定义取数逻辑
stream.jdbcBatchUpdate2(sql, keyNum = 7) {
  value => Seq(value.getName, value.getAge, value.getCreateTime, value.getLength, value.getSex)
}

// Table：按 Row 字段顺序填充占位符
table.jdbcBatchUpdate(sql, keyNum = 10)

// Table：自定义 Row → 参数映射
table.jdbcBatchUpdate2(sql, keyNum = 10) { row =>
  Seq(row.getField(0), row.getField(1), row.getField(2))
}
```

#### [2.4 Flink 多线程 API](../fire-examples/flink-examples/src/test/scala/com/zto/fire/examples/flink/jdbc/JdbcUnitTest.scala)

　　Flink 多线程 JDBC 写入使用独立的 **`JdbcAsyncSink`** 组件（与同步 `JdbcSink` 分离，避免影响已有 `keyNum` 参数传递）。当 `threadNum > 1` 时，每次 flush 将批次数据拆分后调用 `JdbcConnector.updateBatchAsync` 并发写入；`threadNum = 1` 时退化为单线程逻辑。

　　API 命名与 HBase 一致：方法名带 **`Async`** 后缀；参数顺序为 `(..., threadNum, keyNum)`，**`keyNum` 始终在最后**。默认线程数读取配置项 `fire.jdbc.thread.num`（默认 2）。

##### DataStream 多线程 Sink

**（1）指定字段列表**：框架通过反射从 DataStream 元素中取字段值，填充 SQL 占位符。

```scala
// 方式一：DataStream 扩展方法
stream.jdbcBatchUpdateAsync(sql, fields, threadNum = 3, keyNum = 6).setParallelism(3)

// 方式二：fire 入口
this.fire.jdbcBatchUpdateStream(stream, sql, fields, threadNum = 3, keyNum = 6).setParallelism(1)
```

**（2）自定义映射函数**：适用于反射无法取值的场景，灵活性更高。

```scala
// 方式一：DataStream 扩展方法
stream.jdbcBatchUpdateAsync2(sql, threadNum = 3, keyNum = 7) {
  value => Seq(value.getName, value.getAge, DateFormatUtils.formatCurrentDateTime(), value.getLength, value.getSex)
}.setParallelism(1)

// 方式二：fire 入口
this.fire.jdbcBatchUpdateStream2(stream, sql, threadNum = 3, keyNum = 7) {
  value => Seq(value.getName, value.getAge, value.getCreateTime, value.getLength, value.getSex)
}.setParallelism(2)
```

##### Table 多线程 Sink

```scala
// 方式一：按 Row 字段顺序自动填充占位符
table.jdbcBatchUpdateAsync(sql, threadNum = 3, keyNum = 10).setParallelism(1)

// 方式二：自定义 Row → 参数映射
table.jdbcBatchUpdateAsync2(sql, threadNum = 3, keyNum = 10) { row =>
  Seq(row.getField(0), row.getField(1), row.getField(2), row.getField(3), row.getField(4))
}

// 通过 fire 入口
this.fire.jdbcBatchUpdateTable(table, sql, threadNum = 3, keyNum = 10)
this.fire.jdbcBatchUpdateTable2(table, sql, threadNum = 3, keyNum = 10) { row =>
  Seq(row.getField(0), row.getField(1))
}
```

##### 完整调用流程参考

```scala
override def process: Unit = {
  val stream = this.fire.createKafkaDirectStream()
    .filter(t => JSONUtils.isLegal(t))
    .map(json => JSONUtils.parseObject[Student](json))

  // 多线程写入（数据在 job 启动后才落库）
  stream.jdbcBatchUpdateAsync2(sql, threadNum = 3) {
    value => Seq(value.getName, value.getAge, value.getCreateTime)
  }
}
```

　　**Flink 使用注意**：

- **流式 Sink 时序**：`jdbcBatchUpdateAsync` 注册的是 Flink 算子，数据在 job `start()` 后才写入数据库，不宜在 `process()` 中紧接着查询验证。
- **并发度与线程数**：`threadNum` 控制 Sink flush 时的 JDBC 并发写线程数；`setParallelism` 控制 Flink 算子并行度，两者含义不同，需分别设置。
- **连接池限制**：实际并发写线程数不会超过 `db.jdbc.maxPoolSize`。
- **事务语义**：与 Spark 类似，多线程分组各自提交，不保证跨分组的原子性。

##### Spark 与 Flink 多线程 API 对照

| 操作 | Spark | Flink |
| --- | --- | --- |
| Driver 端批量写 | `fire.jdbcUpdateBatchAsync` | — |
| 分布式数据写 | `df.jdbcUpdateBatchAsync` | `stream.jdbcBatchUpdateAsync` / `stream.jdbcBatchUpdateAsync2` |
| Table 写 | — | `table.jdbcBatchUpdateAsync` / `table.jdbcBatchUpdateAsync2` |
| 多线程查询 | `fire.jdbcQueryDFAsync` / `jdbcQueryRDDAsync` | —（在 process 中用同步 `jdbcQueryList`） |
| fire 入口 | `fire.jdbcUpdateBatchDFAsync` | `fire.jdbcBatchUpdateStream(2)` / `fire.jdbcBatchUpdateTable(2)` |

### 三、多个数据源读写

Fire框架支持同一个任务中读写任意个数的数据源，只需要通过keyNum指定即可。配置和使用方式可以参考：HBase、kafka等。

```scala
// Spark 同步写入
this.fire.jdbcUpdate(insertSql, params, keyNum = 2)
df.jdbcUpdateBatch(insertSql, fields, keyNum = 2)

// Spark 多线程写入（keyNum 始终在最后）
this.fire.jdbcUpdateBatchAsync(insertSql, paramList, threadNum = 3, keyNum = 2)
df.jdbcUpdateBatchAsync(insertSql, fields, threadNum = 3, keyNum = 2)

// Flink 多线程 Sink
stream.jdbcBatchUpdateAsync2(sql, threadNum = 3, keyNum = 2) { value => Seq(...) }
```

### 四、@JDBC

```java

/**
 * Jdbc的url，同value
 */
String url();

/**
 * jdbc 驱动类，不填可根据url自动推断
 */
String driver() default "";

/**
 * jdbc的用户名
 */
String username();

/**
 * jdbc的密码
 */
String password() default "";

/**
 * 事务的隔离级别
 */
String isolationLevel() default "";

/**
 * 连接池的最大连接数
 */
int maxPoolSize() default -1;

/**
 * 连接池最少连接数
 */
int minPoolSize() default -1;

/**
 * 连接池初始连接数
 */
int initialPoolSize() default -1;

/**
 * 连接池的增量
 */
int acquireIncrement() default -1;

/**
 * 连接的最大空闲时间
 */
int maxIdleTime() default -1;

/**
 * 多少条操作一次
 */
int batchSize() default -1;

/**
 * flink引擎：flush的间隔周期（ms）
 */
long flushInterval() default -1;

/**
 * flink引擎：失败最大重试次数
 */
int maxRetries() default -1;

/**
 * spark引擎：scan后的缓存级别：fire.jdbc.storage.level
 */
String storageLevel() default "";

/**
 * spark引擎：select后存放到rdd的多少个partition中：fire.jdbc.query.partitions
 */
int queryPartitions() default -1;

/**
 * 日志中打印的sql长度
 */
int logSqlLength() default -1;

/**
 * c3p0参数，以key=value形式注明
 */
String[] config() default "";
```

### 五、配置参数

列表中的配置参数可根据需要放到任务的配置文件中。

| 参数名称                 | 引擎 | 含义                                     |
| ------------------------ | ---- | ---------------------------------------- |
| db.jdbc.url              | 通用 | jdbc url                                 |
| db.jdbc.url.map.         | 通用 | 用于为url取别名                          |
| db.jdbc.driver           | 通用 | driver class                             |
| db.jdbc.user             | 通用 | 数据库用户名                             |
| db.jdbc.password         | 通用 | 数据库密码                               |
| db.jdbc.isolation.level  | 通用 | 事务的隔离级别                           |
| db.jdbc.maxPoolSize      | 通用 | 连接池最大连接数                         |
| db.jdbc.minPoolSize      | 通用 | 连接池最小连接数                         |
| db.jdbc.acquireIncrement | 通用 | 当连接池连接数不足时，增量申请连接数大小 |
| fire.jdbc.thread.num     | 通用 | JDBC 多线程读写并发线程数，默认 2        |

