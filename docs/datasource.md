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

# Spark DataSource增强

　　Spark DataSource API很强大，为了进一步增强灵活性，Fire框架针对DataSource API做了进一步封装，允许将options等信息放到配置文件中，提高灵活性，如果与实时平台的配置中心集成，可做到重启即完成调优。

[示例程序：](../fire-examples/spark-examples/src/main/scala/com/zto/fire/examples/spark/datasource/DataSourceTest.scala)

```scala
val ds = this.fire.createDataFrame(Student.newStudentList(), classOf[Student])
ds.createOrReplaceTempView("test")

val dataFrame = this.fire.sql("select * from test")

// 一、 dataFrame.write.format.mode.save中的所有参数均可通过配置文件指定
// dataFrame.writeEnhance()

// 二、 dataFrame.write.mode.save中部分参数通过配置文件指定，或全部通过方法硬编码指定
val savePath = "/user/hive/warehouse/hudi.db/hudi_bill_event_test"

// 如果代码中与配置文件中均指定了option，则相同的options配置文件优先级更高，不同的option均生效
val options = Map(
    "hoodie.datasource.write.recordkey.field" -> "id",
    "hoodie.datasource.write.precombine.field" -> "id"
)

// 使用keyNum标识读取配置文件中不同配置后缀的options信息
// dataFrame.writeEnhance("org.apache.hudi", SaveMode.Append, savePath, options = options, keyNum = 2)

// read.format.mode.load(path)
this.fire.readEnhance(keyNum = 3)
```

配置文件：

```properties
# 一、hudi datasource，全部基于配置文件进行配置
spark.datasource.format=org.apache.hudi
spark.datasource.saveMode=Append
# 用于区分调用save(path)还是saveAsTable
spark.datasource.isSaveTable=false
# 传入到底层save或saveAsTable方法中
spark.datasource.saveParam=/user/hive/warehouse/hudi.db/hudi_bill_event_test

# 以spark.datasource.options.为前缀的配置用于配置hudi相关的参数，可覆盖代码中同名的配置
spark.datasource.options.hoodie.datasource.write.recordkey.field=id
spark.datasource.options.hoodie.datasource.write.precombine.field=id
spark.datasource.options.hoodie.datasource.write.partitionpath.field=ds
spark.datasource.options.hoodie.table.name=hudi.hudi_bill_event_test
spark.datasource.options.hoodie.datasource.write.hive_style_partitioning=true
spark.datasource.options.hoodie.datasource.write.table.type=MERGE_ON_READ
spark.datasource.options.hoodie.insert.shuffle.parallelism=128
spark.datasource.options.hoodie.upsert.shuffle.parallelism=128
spark.datasource.options.hoodie.fail.on.timeline.archiving=false
spark.datasource.options.hoodie.clustering.inline=true
spark.datasource.options.hoodie.clustering.inline.max.commits=8
spark.datasource.options.hoodie.clustering.plan.strategy.target.file.max.bytes=1073741824
spark.datasource.options.hoodie.clustering.plan.strategy.small.file.limit=629145600
spark.datasource.options.hoodie.clustering.plan.strategy.daybased.lookback.partitions=2

# 二、配置第二个数据源，以数字后缀作为区分，部分使用配置文件进行配置
spark.datasource.format2=org.apache.hudi2
spark.datasource.saveMode2=Overwrite
# 用于区分调用save(path)还是saveAsTable
spark.datasource.isSaveTable2=false
# 传入到底层save或saveAsTable方法中
spark.datasource.saveParam2=/user/hive/warehouse/hudi.db/hudi_bill_event_test2

# 三、配置第三个数据源，用于代码中进行read操作
spark.datasource.format3=org.apache.hudi3
spark.datasource.loadParam3=/user/hive/warehouse/hudi.db/hudi_bill_event_test3
spark.datasource.options.hoodie.datasource.write.recordkey.field3=id3
```

