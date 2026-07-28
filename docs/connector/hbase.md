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

# HBase 读写

　　HBase对更新和点查具有很好的支持，在实时计算场景下也是应用十分广泛的。为了进一步简化HBase读写api，提高开发效率，fire框架对HBase API进行了深度封装。目前支持3种读写模式，分别是：Java API、Bulk API以及Spark提供的API；其中 Java API 还提供**多线程并发读写**能力（方法名带 `Async` 后缀），适用于中等数据量的高吞吐场景。另外，fire框架支持在同一个任务中对任意多个hbase集群同时进行读写。

## 一、HBase集群配置

### 1.1 定义别名

建议将hbase集群url信息定义成别名，别名定义放到名为common.properties的配置文件中。别名的好处是一处维护到处生效，方便共用，便于记忆。

```properties
# 定义hbase集群连接信息别名为test，代码中hbase配置简化为：@HBase("test")
fire.hbase.cluster.map.test		=			zk01:2181,zk02:2181,zk03:2181
```

### 1.2 基于注解配置

```scala
@HBase("zk01:2181,zk02:2181,zk03:2181")
@HBase2(cluster = "test", scanPartitions = 3, storageLevel = "DISK_ONLY")
```

### 1.3 基于配置文件

```properties
# 方式一：直接指定zkurl
hbase.cluster									=				zkurl
# 方式二：事先定义好hbase别名与url的映射，然后通过别名配置，以下配置定义了别名test与url的映射关系
fire.hbase.cluster.map.test		=				zk01:2181,zk02:2181,zk03:2181
# 通过别名方式引用
hbase.cluster2								=				test
```

## 二、表与JavaBean映射

Fire框架通过Javabean与HBase表建立的关系简化读写api：

```java
/**
 * 对应HBase表的JavaBean
 *
 * @author ChengLong 2019-6-20 16:06:16
 */
@HConfig(multiVersion = true)
public class Student extends HBaseBaseBean<Student> {
    private Long id;
    private String name;
    private Integer age;
    // 多列族情况下需使用family单独指定
    private String createTime;
    // 若JavaBean的字段名称与HBase中的字段名称不一致，需使用value单独指定
    // 此时hbase中的列名为length1，而不是length
    @FieldName(family = "data", value = "length1")
    private BigDecimal length;
    private Boolean sex;

    /**
     * rowkey的构建
     *
     * @return
     */
    @Override
    public Student buildRowKey() {
        this.rowKey = this.id.toString();
        return this;
    }
}

```

　　上述代码中定义了名为Student的Javabean，该Javabean需要继承自HBaseBaseBean，并实现buildRowKey方法，这个方法中需要告诉fire框架，rowKey是如何构建的。

　　通过以上两步即可实现Javabean与HBase表的关系绑定。对于个性化需求，如果需要以多版本的方式进行读写，则需在类名上添加@HConfig(multiVersion = true)注解。如果Javabean中的列名与HBase中的字段名不一致，可以通过@FieldName(family = "data", value = "length1")进行单独指定，当然，列族也可以通过这个注解指定。如果不知道列族名称，则默认只有一个名为info的列族。

目前暂不支持scala语言的class以及case class，仅支持基本的字段数据类型，不支持嵌套的或者复杂的字段类型。

## 三、spark任务

### [1.1 java api](../fire-examples/spark-examples/src/main/scala/com/zto/fire/examples/spark/hbase/HBaseConnectorTest.scala)

```scala
/**
  * 使用HBaseConnector插入一个rdd的数据
  * rdd的类型必须为HBaseBaseBean的子类
  */
def testHbasePutRDD: Unit = {
    val studentList = Student.newStudentList()
    val studentRDD = this.fire.createRDD(studentList, 2)
    // 为空的字段不插入
    studentRDD.hbasePutRDD(this.tableName1)
}

/**
  * 使用HBaseConnector插入一个DataFrame的数据
  */
def testHBasePutDF: Unit = {
    val studentList = Student.newStudentList()
    val studentDF = this.fire.createDataFrame(studentList, classOf[Student])
    // 每个批次插100条
    studentDF.hbasePutDF(this.tableName1, classOf[Student])
}

/**
  * 使用HBaseConnector get数据，并将结果以RDD方式返回
  */
def testHbaseGetRDD: Unit = {
    val getList = Seq("1", "2", "3", "5", "6")
    val getRDD = this.fire.createRDD(getList, 2)
    // 以多版本方式get，并将结果集封装到rdd中返回
    val studentRDD = this.fire.hbaseGetRDD(this.tableName1, classOf[Student], getRDD)
    studentRDD.printEachPartition
}

/**
  * 使用HBaseConnector get数据，并将结果以DataFrame方式返回
  */
def testHbaseGetDF: Unit = {
    val getList = Seq("1", "2", "3", "4", "5", "6")
    val getRDD = this.fire.createRDD(getList, 3)
    // get到的结果以dataframe形式返回
    val studentDF = this.fire.hbaseGetDF(this.tableName1, classOf[Student], getRDD)
    studentDF.show(100, false)
}

/**
  * 使用HBaseConnector scan数据，并以RDD方式返回
  */
def testHbaseScanRDD: Unit = {
    val rdd = this.fire.hbaseScanRDD2(this.tableName1, classOf[Student], "1", "6")
    rdd.repartition(3).printEachPartition
}

/**
  * 使用HBaseConnector scan数据，并以DataFrame方式返回
  */
def testHbaseScanDF: Unit = {
    val dataFrame = this.fire.hbaseScanDF2(this.tableName1, classOf[Student], "1", "6")
    dataFrame.repartition(3).show(100, false)
}
```

### [1.2 bulk api](../fire-examples/spark-examples/src/main/scala/com/zto/fire/examples/spark/hbase/HbaseBulkTest.scala)

```scala
/**
  * 使用bulk的方式将rdd写入到hbase
  */
def testHbaseBulkPutRDD: Unit = {
    // 方式一：将rdd的数据写入到hbase中，rdd类型必须为HBaseBaseBean的子类
    val rdd = this.fire.createRDD(Student.newStudentList(), 2)
    // rdd.hbaseBulkPutRDD(this.tableName2)
    // 方式二：使用this.fire.hbaseBulkPut将rdd中的数据写入到hbase
    this.fire.hbaseBulkPutRDD(this.tableName2, rdd)

    // 第二个参数指定false表示不插入为null的字段到hbase中
    // rdd.hbaseBulkPutRDD(this.tableName2, insertEmpty = false)
    // 第三个参数为true表示以多版本json格式写入
    // rdd.hbaseBulkPutRDD(this.tableName3, false, true)
}

/**
  * BulkLoad：写 HFile 并 LoadIncrementalHFiles 入库
  * 需配置 fire.hbase.bulkload.stagingDir（或 @HConfig.bulkLoadStagingDir）
  * staging 需为 HBase 侧 HDFS URI；@HBase("alias") 会自动加载 hdfs.ha.conf.alias.*
  */
def testHbaseBulkLoadRDD: Unit = {
    val rdd = this.fire.createRDD(Student.newStudentList(), 2)
    // 方式一
    this.fire.hbaseBulkLoadRDD[Student](this.tableName20, rdd)
    // 方式二
    // rdd.hbaseBulkLoadRDD[Student](this.tableName20)
}

/**
  * 使用bulk的方式将DataFrame写入到hbase
  */
def testHbaseBulkPutDF: Unit = {
    // 方式一：将DataFrame的数据写入到hbase中
    val rdd = this.fire.createRDD(Student.newStudentList(), 2)
    val studentDF = this.fire.createDataFrame(rdd, classOf[Student])
    // insertEmpty=false表示为空的字段不插入
    studentDF.hbaseBulkPutDF(this.tableName1, classOf[Student], keyNum = 2)
    // 方式二：
    // this.fire.hbaseBulkPutDF(this.tableName2, studentDF, classOf[Student])
}

/**
  * 使用bulk方式根据rowKey获取数据，并将结果集以RDD形式返回
  */
def testHBaseBulkGetRDD: Unit = {
    // 方式一：使用rowKey读取hbase中的数据，rowKeyRdd类型为String
    val rowKeyRdd = this.fire.createRDD(Seq(1.toString, 2.toString, 3.toString, 5.toString, 6.toString), 2)
    val studentRDD = rowKeyRdd.hbaseBulkGetRDD(this.tableName1, classOf[Student], keyNum = 2)
    studentRDD.foreach(println)
    // 方式二：使用this.fire.hbaseBulkGetRDD
    // val studentRDD2 = this.fire.hbaseBulkGetRDD(this.tableName2, rowKeyRdd, classOf[Student])
    // studentRDD2.foreach(println)
}

/**
  * 使用bulk方式根据rowKey获取数据，并将结果集以DataFrame形式返回
  */
def testHBaseBulkGetDF: Unit = {
    // 方式一：使用rowKey读取hbase中的数据，rowKeyRdd类型为String
    val rowKeyRdd = this.fire.createRDD(Seq(1.toString, 2.toString, 3.toString, 5.toString, 6.toString), 2)
    val studentDF = rowKeyRdd.hbaseBulkGetDF(this.tableName2, classOf[Student])
    studentDF.show(100, false)
    // 方式二：使用this.fire.hbaseBulkGetDF
    val studentDF2 = this.fire.hbaseBulkGetDF(this.tableName2, rowKeyRdd, classOf[Student])
    studentDF2.show(100, false)
}

/**
  * 使用bulk方式进行scan，并将结果集映射为RDD
  */
def testHbaseBulkScanRDD: Unit = {
    // scan操作，指定rowKey的起止或直接传入自己构建的scan对象实例，返回类型为RDD[Student]
    val scanRDD = this.fire.hbaseBulkScanRDD2(this.tableName2, classOf[Student], "1", "6")
    scanRDD.foreach(println)
}

/**
  * 使用bulk方式进行scan，并将结果集映射为DataFrame
  */
def testHbaseBulkScanDF: Unit = {
    // scan操作，指定rowKey的起止或直接传入自己构建的scan对象实例，返回类型为DataFrame
    val scanDF = this.fire.hbaseBulkScanDF2(this.tableName2, classOf[Student], "1", "6")
    scanDF.show(100, false)
}
```

### [1.3 spark api](../fire-examples/spark-examples/src/main/scala/com/zto/fire/examples/spark/hbase/HBaseHadoopTest.scala)

```scala
/**
  * 基于saveAsNewAPIHadoopDataset封装，将rdd数据保存到hbase中
  */
def testHbaseHadoopPutRDD: Unit = {
    val studentRDD = this.fire.createRDD(Student.newStudentList(), 2)
    this.fire.hbaseHadoopPutRDD(this.tableName2, studentRDD, keyNum = 2)
    // 方式二：直接基于rdd进行方法调用
    // studentRDD.hbaseHadoopPutRDD(this.tableName1)
}

/**
  * 基于saveAsNewAPIHadoopDataset封装，将DataFrame数据保存到hbase中
  */
def testHbaseHadoopPutDF: Unit = {
    val studentRDD = this.fire.createRDD(Student.newStudentList(), 2)
    val studentDF = this.fire.createDataFrame(studentRDD, classOf[Student])
    // 由于DataFrame相较于Dataset和RDD是弱类型的数据集合，所以需要传递具体的类型classOf[Type]
    this.fire.hbaseHadoopPutDF(this.tableName3, studentDF, classOf[Student])
    // 方式二：基于DataFrame进行方法调用
    // studentDF.hbaseHadoopPutDF(this.tableName3, classOf[Student])
}

/**
  * 使用Spark的方式scan海量数据，并将结果集映射为RDD
  */
def testHBaseHadoopScanRDD: Unit = {
    val studentRDD = this.fire.hbaseHadoopScanRDD2(this.tableName2, classOf[Student], "1", "6", keyNum = 2)
    studentRDD.printEachPartition
}

/**
  * 使用Spark的方式scan海量数据，并将结果集映射为DataFrame
  */
def testHBaseHadoopScanDF: Unit = {
    val studentDF = this.fire.hbaseHadoopScanDF2(this.tableName3, classOf[Student], "1", "6")
    studentDF.show(100, false)
}
```

### [1.4 多线程 API](../fire-examples/spark-examples/src/main/scala/com/zto/fire/examples/spark/hbase/HBaseConnectorTest.scala)

　　Fire 框架在 Java API（`HBaseConnector`）之上封装了多线程读写能力。底层 HBase `Connection` 本身是线程安全的，框架会在同一连接上并发执行 Put / Get / Scan，以提升大批量操作的吞吐。

　　多线程 API 与同步 API 一一对应，方法名统一带 **`Async`** 后缀；同步 API 签名保持不变，不影响已有代码。新增参数 **`threadNum`** 表示并发线程数，**`keyNum` 始终放在参数列表最后**（Fire 框架统一约定）。

　　可通过配置文件指定默认并发线程数（不指定时默认 2）：

```properties
# 全局默认 HBase 并发线程数
fire.hbase.thread.num=3
# 指定 keyNum=2 集群的并发线程数
fire.hbase.thread.num2=4
```

#### Put 写入

**（1）集合写入**：适用于 Driver 端已有 `List` / `Set` 等集合的场景，数据在 Driver 端按线程数分组后并发写入。

```scala
def testHbasePutListAsync(threadNum: Int): Unit = {
  val studentList = Student.newStudentList()
  // 参数顺序：表名、线程数、数据集合、keyNum（可选，默认 1）
  this.fire.hbasePutListAsync[Student](this.tableName13, threadNum, studentList)
}
```

**（2）RDD 写入**：每个分区内的数据再按 `threadNum` 分组并发写入，适合 Spark 分布式场景。

```scala
def testHbasePutRDDAsync(threadNum: Int): Unit = {
  val studentRDD = this.fire.createRDD(Student.newStudentList(), 2)
  // RDD 扩展方法：表名、线程数、keyNum（可选）
  studentRDD.hbasePutRDDAsync[Student](this.tableName14, threadNum)
}
```

**（3）DataFrame 写入**：

```scala
def testHBasePutDFAsync(threadNum: Int): Unit = {
  val studentDF = this.fire.createDataFrame(Student.newStudentList(), classOf[Student])
  studentDF.hbasePutDFAsync[Student](this.tableName15, threadNum)
}
```

**（4）Dataset 写入**：

```scala
def testHBasePutDSAsync(threadNum: Int): Unit = {
  val studentDS = this.fire.createDataset(Student.newStudentList())(Encoders.bean(classOf[Student]))
  studentDS.hbasePutDSAsync[Student](this.tableName16, threadNum)
}
```

#### Get 读取

　　多线程 Get 在 Driver 端执行，将 rowKey 列表拆分后并发查询，结果合并为 `Seq` 返回。提供两种入参方式：

```scala
def testHbaseGetListAsync(tableName: String, threadNum: Int): Unit = {
  val rowKeys = Seq("1", "2", "3", "5", "6")

  // 方式一：直接传入 rowKey 字符串列表
  val studentList = this.fire.hbaseGetListAsync2[Student](tableName, threadNum, rowKeys)
  studentList.foreach(println)

  // 方式二：传入 HBase Get 对象列表（可自定义版本数等参数）
  val getList = ListBuffer[Get]()
  rowKeys.foreach(rowKey => getList += new Get(rowKey.getBytes(StandardCharsets.UTF_8)))
  val studentList2 = this.fire.hbaseGetListAsync[Student](tableName, threadNum, getList)
  studentList2.foreach(println)
}
```

#### Scan 扫描

　　多线程 Scan 会将 rowKey 区间按线程数切分后并发扫描，适合较大区间的点查场景（区别于 Bulk / Hadoop API 的海量 scan）。

```scala
def testHbaseScanListAsync(tableName: String, threadNum: Int): Unit = {
  // 扫描 [startRow, stopRow) 左闭右开区间
  val list = this.fire.hbaseScanListAsync2[Student](tableName, threadNum, "1", "6")
  list.foreach(println)
}
```

#### 完整调用流程参考

```scala
override def process: Unit = {
  // 1. 多线程写入
  this.testHbasePutListAsync(3)
  this.testHbasePutRDDAsync(2)
  this.testHBasePutDFAsync(3)
  this.testHBasePutDSAsync(3)

  // 2. 写入完成后，多线程 Get / Scan 验证
  this.testHbaseGetListAsync(this.tableName13, 2)
  this.testHbaseScanListAsync(this.tableName13, 2)
}
```

　　**使用建议**：

- 数据量较小、实时性要求高的场景，优先使用多线程 Java API；海量数据导入仍推荐使用 Bulk API 或 Spark API。
- `threadNum` 建议根据 HBase 集群负载和数据量调整，一般 2~8 即可，不宜过大。
- 多集群场景下通过 `keyNum` 指定目标集群，写法与同步 API 完全一致。

## 四、flink任务

### [4.1 同步 API（单线程 Sink）](../fire-examples/flink-examples/src/main/scala/com/zto/fire/examples/flink/connector/hbase/HBaseTest.scala)

　　Flink 任务中 HBase 写入通过 Sink 组件实现，同步 API 底层使用 `HBaseSink`，每次 flush 单线程批量写入。Get / Scan / Delete 等读操作可直接在 `process()` 中调用 `HBaseConnector`。

```scala
/**
  * table的hbase sink
  */
def testTableHBaseSink(stream: DataStream[Student]): Unit = {
    stream.createOrReplaceTempView("student")
    val table = this.flink.sqlQuery("select id, name, age from student group by id, name, age")
    // 方式一、自动将row转为对应的JavaBean
    // 注意：table对象上调用hbase api，需要指定泛型
    table.hbasePutTable[Student](this.tableName).setParallelism(1)
    this.fire.hbasePutTable[Student](table, this.tableName2, keyNum = 2)

    // 方式二、用户自定义取数规则，从row中创建HBaseBaseBean的子类
    table.hbasePutTable2[Student](this.tableName3)(row => new Student(1L, row.getField(1).toString, row.getField(2).toString.toInt))
    // 或者
    this.fire.hbasePutTable2[Student](table, this.tableName5, keyNum = 2)(row => new Student(1L, row.getField(1).toString, row.getField(2).toString.toInt))
}

/**
  * table的hbase sink
  */
def testTableHBaseSink2(stream: DataStream[Student]): Unit = {
    val table = this.fire.sqlQuery("select id, name, age from student group by id, name, age")

    // 方式二、用户自定义取数规则，从row中创建HBaseBaseBean的子类
    table.hbasePutTable2(this.tableName6)(row => new Student(1L, row.getField(1).toString, row.getField(2).toString.toInt))
    // 或者
    this.flink.hbasePutTable2(table, this.tableName7, keyNum = 2)(row => new Student(1L, row.getField(1).toString, 		   row.getField(2).toString.toInt))
}

/**
  * stream hbase sink
  */
def testStreamHBaseSink(stream: DataStream[Student]): Unit = {
    // 方式一、DataStream中的数据类型为HBaseBaseBean的子类
    // stream.hbasePutDS(this.tableName)
    this.fire.hbasePutDS[Student](stream, this.tableName8)

    // 方式二、将value组装为HBaseBaseBean的子类，逻辑用户自定义
    stream.hbasePutDS2(this.tableName9, keyNum = 2)(value => value)
    // 或者
    this.fire.hbasePutDS2(stream, this.tableName10)(value => value)
}

/**
  * stream hbase sink
  */
def testStreamHBaseSink2(stream: DataStream[Student]): Unit = {
    // 方式二、将value组装为HBaseBaseBean的子类，逻辑用户自定义
    stream.hbasePutDS2(this.tableName11)(value => value)
    // 或者
    this.fire.hbasePutDS2(stream, this.tableName12, keyNum = 2)(value => value)
}

/**
  * hbase的基本操作
  */
def testHBase: Unit = {
    // get操作
    val getList = ListBuffer(HBaseConnector.buildGet("1"))
    val student = HBaseConnector.get(this.tableName, classOf[Student], getList, 1)
    if (student != null) println(JSONUtils.toJSONString(student))
    // scan操作
    val studentList = HBaseConnector.scan(this.tableName, classOf[Student], HBaseConnector.buildScan("0", "9"), 1)
    if (studentList != null) println(JSONUtils.toJSONString(studentList))
    // delete操作
    HBaseConnector.deleteRows(this.tableName, Seq("1"))
}
```

### [4.2 多线程 API](../fire-examples/flink-examples/src/main/scala/com/zto/fire/examples/flink/connector/hbase/HBaseTest.scala)

　　Flink 多线程 API 同样基于 `HBaseConnector` 的 `insertAsync` / `getAsync` / `scanAsync` 实现。流式写入场景使用独立的 **`HBaseAsyncSink`** 组件（与同步 `HBaseSink` 分离，避免影响已有 `keyNum` 参数传递）。

　　API 命名规则与 Spark 一致：方法名带 **`Async`** 后缀；Sink 类 API 参数顺序为 `(..., threadNum, keyNum)`，**`keyNum` 始终在最后**。默认线程数读取配置项 `fire.hbase.thread.num`（默认 2）。

#### Put 写入

**（1）集合写入**：在 `process()` 中直接调用，数据立即写入，适合测试数据准备或批量补数。

```scala
// 参数顺序：表名、线程数、数据集合、keyNum（可选）
this.fire.hbasePutListAsync[Student](this.tableName14, 2, Student.newStudentList().asScala)
```

**（2）DataStream 写入**：对应 Spark 的 RDD Put。`DataStream` 元素类型为 `HBaseBaseBean` 子类时，可直接写入。

```scala
def testStreamHBasePutAsync(stream: DataStream[Student], threadNum: Int): Unit = {
  // 通过 fire 入口调用
  this.fire.hbasePutDSAsync[Student](stream, this.tableName14, threadNum = threadNum)
}
```

**（3）DataStream 自定义映射写入**：对应 Spark 的 Dataset Put，通过函数将流元素转为 HBase Bean。

```scala
def testStreamHBasePutAsync2(stream: DataStream[Student], threadNum: Int): Unit = {
  stream.hbasePutDSAsync2(this.tableName16, threadNum = threadNum)(value => value)
}
```

**（4）Table 写入**：对应 Spark 的 DataFrame Put。Table 需先转为 Retract 流再 Sink。

```scala
def testTableHBasePutAsync(stream: DataStream[Student], threadNum: Int): Unit = {
  stream.createOrReplaceTempView("student_async_df")
  val table = this.fire.sqlQuery("select id, name, age from student_async_df group by id, name, age")
  // 方式一：自动将 Row 映射为 JavaBean
  table.hbasePutTableAsync[Student](this.tableName15, threadNum = threadNum).setParallelism(1)
}
```

**（5）Table 自定义映射写入**：

```scala
def testTableHBaseSinkAsync(stream: DataStream[Student], threadNum: Int): Unit = {
  stream.createOrReplaceTempView("student")
  val table = this.fire.sqlQuery("select id, name, age from student group by id, name, age")
  table.hbasePutTableAsync[Student](this.tableName15, threadNum = threadNum).setParallelism(1)
  // 方式二：自定义 Row → JavaBean 映射规则
  table.hbasePutTableAsync2[Student](this.tableName16, keyNum = 2, threadNum = threadNum) {
    row => new Student(row.getField(0).toString.toLong, row.getField(1).toString, row.getField(2).toString.toInt)
  }
}
```

**（6）Kafka 流多线程 Sink**：与同步 Sink 用法一致，仅将方法替换为 Async 版本。

```scala
def testStreamHBaseSinkAsync(stream: DataStream[Student], threadNum: Int): Unit = {
  stream.hbasePutDSAsync2(this.tableName13, threadNum = threadNum)(value => value)
}
```

#### Get 读取

　　Get 在 `process()` 中同步执行并立即返回结果，用法与 Spark 一致：

```scala
def testHbaseGetListAsync(tableName: String, threadNum: Int): Unit = {
  val rowKeys = Seq("1", "2", "3", "5", "6")

  // 方式一：传入 rowKey 字符串列表
  val studentList = this.fire.hbaseGetListAsync2[Student](tableName, threadNum, rowKeys)
  studentList.foreach(println)

  // 方式二：传入 Get 对象列表
  val getList = ListBuffer[Get]()
  rowKeys.foreach(rowKey => getList += new Get(rowKey.getBytes(StandardCharsets.UTF_8)))
  val studentList2 = this.fire.hbaseGetListAsync[Student](tableName, threadNum, getList)
  studentList2.foreach(println)
}
```

#### Scan 扫描

```scala
def testHbaseScanListAsync(tableName: String, threadNum: Int): Unit = {
  val list = this.fire.hbaseScanListAsync2[Student](tableName, threadNum, "1", "6")
  list.foreach(println)
}
```

#### 完整调用流程参考

```scala
override def process: Unit = {
  val stream = this.fire.createRandomIntStream(1).flatMap(t => Student.newStudentList()).setParallelism(1)

  // 1. 多线程流式 Sink（数据在 job 启动后才写入 HBase）
  this.testStreamHBaseSinkAsync(stream, 3)
  this.testHbaseGetListAsync(this.tableName13, 2)
  this.testHbaseScanListAsync(this.tableName13, 2)

  // 2. 流式 Sink 的数据在 job 启动后才落盘，Get/Scan 验证前需先用集合 Put 准备数据
  this.fire.hbasePutListAsync[Student](this.tableName14, 2, Student.newStudentList().asScala)
  this.testHbaseGetListAsync(this.tableName14, 2)
  this.testHbaseScanListAsync(this.tableName14, 2)
}
```

　　**Flink 使用注意**：

- **流式 Sink 与读操作的时序**：`hbasePutDSAsync` / `hbasePutTableAsync` 等 Sink API 注册的是 Flink 算子，数据在 job `start()` 后才真正写入 HBase。若在 `process()` 中紧接着做 Get/Scan 验证，需先用 `hbasePutListAsync` 同步写入测试数据，或等 job 运行后再验证。
- **Get / Scan 可直接在 process 中调用**：与 Spark Driver 端行为一致，调用后立即返回结果。
- **多集群**：通过 `keyNum` 指定，例如 `stream.hbasePutDSAsync2(tableName, keyNum = 2, threadNum = 3)(...)`。

#### Spark 与 Flink 多线程 API 对照

| 操作 | Spark | Flink |
| --- | --- | --- |
| 集合 Put | `fire.hbasePutListAsync` | `fire.hbasePutListAsync` |
| 分布式数据 Put | `rdd.hbasePutRDDAsync` / `df.hbasePutDFAsync` / `ds.hbasePutDSAsync` | `stream.hbasePutDSAsync` / `stream.hbasePutDSAsync2` / `table.hbasePutTableAsync` |
| Get | `fire.hbaseGetListAsync` / `hbaseGetListAsync2` | `fire.hbaseGetListAsync` / `hbaseGetListAsync2` |
| Scan | `fire.hbaseScanListAsync2` | `fire.hbaseScanListAsync2` |

## 五、多集群读写

Fire框架支持同一个任务中对任意多个hbase集群进行读写，首先要在配置文件中以keyNum进行指定要连接的所有hbase集群的zk地址：

```scala
@HBase("zk01:2181")
@HBase2("zk02:2181")
@HBase3("zk03:2181")
```

```properties
hbase.cluster=zk01:2181
hbase.cluster3=zk02:2181
hbase.cluster8=zk03:2181
```

在代码中，通过keyNum参数告诉fire这行代码连接的hbase集群是哪个。注意：api中的keyNum要与配置中的数字对应上。

```scala
// insert 操作（同步）
studentRDD.hbasePutRDD(this.tableName1)
studentRDD.hbasePutRDD(this.tableName2, keyNum = 3)
studentRDD.hbasePutRDD(this.tableName3, keyNum = 8)

// insert 操作（多线程，keyNum 始终在最后）
studentRDD.hbasePutRDDAsync[Student](this.tableName1, threadNum = 3)
studentRDD.hbasePutRDDAsync[Student](this.tableName2, threadNum = 3, keyNum = 2)

// scan 操作
this.fire.hbaseScanDF2(this.tableName1, classOf[Student], "1", "6")
this.fire.hbaseScanDF2(this.tableName1, classOf[Student], "1", "6", keyNum = 3)
this.fire.hbaseScanListAsync2[Student](this.tableName1, threadNum = 2, "1", "6", keyNum = 2)
```

## 六、@HBase

```java
/**
 * HBase集群连接信息：hbase.cluster
 */
String value() default "";

/**
 * HBase集群连接信息：hbase.cluster，同value
 */
String cluster() default "";

/**
 * 列族名称：hbase.column.family
 */
String family() default "";

/**
 * 每个线程最多insert的记录数：fire.hbase.batch.size
 */
int batchSize() default -1;

/**
 * spark引擎：scan hbase后存放到rdd的多少个partition中：fire.hbase.scan.partitions
 */
int scanPartitions() default -1;

/**
 * spark引擎：scan后的缓存级别：fire.hbase.storage.level
 */
String storageLevel() default "";

/**
 * flink引擎：sink hbase失败最大重试次数：hbase.max.retry
 */
int maxRetries() default -1;

/**
 * WAL等级：hbase.durability
 */
String durability() default "";

/**
 * 是否启用表信息缓存，提高表是否存在判断的效率：fire.hbase.table.exists.cache.enable
 */
boolean tableMetaCache() default true;

/**
 * hbase-client参数，以key=value形式注明
 */
String[] config() default "";
```

## 七、hbase-client参数

hbase-client参数，可以通过@HBase的**config**或以**fire.hbase.conf.**为前缀的参数去指定：

```scala
@HBase(cluster = "test", config = Array[String]("hbase.rpc.timeout=60000ms", "hbase.client.scanner.timeout.period=60000ms"))
```

```properties
fire.hbase.conf.hbase.rpc.timeout										=				60000ms
fire.hbase.conf.hbase.client.scanner.timeout.period	=				60000ms
```

### HDFS HA（与 @Hive 共用）

`@HBase("fat")` 会按集群别名自动加载 `cluster.properties` 中的 `hdfs.ha.conf.fat.*`（规则与 `@Hive("fat")` 相同），写入 HBase Configuration，供 BulkLoad 等需要访问 HBase 侧 HDFS 的场景使用。无需在任务里再手写 HA。

```properties
# cluster.properties（别名 fat 与 fire.hbase.cluster.map.fat / fire.hive.cluster.map.fat 对应）
hdfs.ha.conf.fat.fs.defaultFS=hdfs://fat-batch
hdfs.ha.conf.fat.dfs.nameservices=fat-batch
hdfs.ha.conf.fat.dfs.ha.namenodes.fat-batch=nn1,nn2
...
```

| 参数名称                                    | 引擎  | 含义                                                   |
| ------------------------------------------- | ----- | ------------------------------------------------------ |
| fire.hbase.batch.size                       | 通用  | insert的批次大小，用于限制单个task一次最多sink的记录数 |
| fire.hbase.thread.num                       | 通用  | HBase Java API 多线程读写并发线程数，默认 2            |
| fire.hbase.bulkload.stagingDir              | spark | BulkLoad HFile staging 目录前缀（必填，无默认值）      |
| hbase.column.family                         | 通用  | 用于配置列族名称，默认info                             |
| hbase.max.retry                             | flink | 当插入失败后，重试多少次                               |
| hbase.cluster                               | 通用  | 所需读写的Hbase集群url或别名                           |
| hbase.durability                            | 通用  | Hbase-client中的durability                             |
| fire.hbase.storage.level                    | spark | 诊断scan后数据的缓存，避免重复scan hbase               |
| fire.hbase.scan.partitions                  | Spark | 通过HBase scan后repartition的分区数                    |
| fire.hbase.cluster.map.                     | 通用  | hbase集群映射配置前缀                                  |
| fire.hbase.table.exists.cache.enable        | 通用  | 是否开启HBase表存在判断的缓存                          |
| fire.hbase.table.exists.cache.reload.enable | 通用  | 是否开启HBase表存在列表缓存的定时更新任务              |
| fire.hbase.table.exists.cache.initialDelay  | 通用  | 定时刷新缓存HBase表任务的初始延迟                      |
| fire.hbase.table.exists.cache.period        | 通用  | 定时刷新缓存HBase表任务的执行频率                      |
| fire.hbase.conf.                            | 通用  | hbase java api 配置前缀，支持任意hbase-client的参数    |

