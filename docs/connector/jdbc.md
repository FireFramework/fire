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

　　实时任务开发中，对jdbc读写的需求很高。为了简化jdbc开发步骤，fire框架对jdbc操作做了进一步封装，将许多常见操作简化成一行代码。另外，fire框架支持在同一个任务中对任意多个数据源进行读写。

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

#### [2.1 spark任务](../fire-examples/spark-examples/src/main/scala/com/zto/fire/examples/spark/jdbc/JdbcTest.scala)

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

#### [2.2 flink任务](../fire-examples/flink-examples/src/main/scala/com/zto/fire/examples/flink/stream/JdbcTest.scala)

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
    // * Flink1.12不支持该API
    dstream.sinkJdbcExactlyOnce(sql, keyNum = 2)
  }
}
```

### 三、多个数据源读写

Fire框架支持同一个任务中读写任意个数的数据源，只需要通过keyNum指定即可。配置和使用方式可以参考：HBase、kafka等。

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

