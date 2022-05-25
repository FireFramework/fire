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

### 依赖管理

```xml
<dependency>
  <groupId>com.zto.fire</groupId>
  <artifactId>fire-common_${scala.binary.version}</artifactId>
  <version>${fire.version}</version>
</dependency>
<dependency>
  <groupId>com.zto.fire</groupId>
  <artifactId>fire-core_${scala.binary.version}</artifactId>
  <version>${fire.version}</version>
</dependency>

<!-- spark任务单独引入fire-spark依赖 -->
<dependency>
  <groupId>com.zto.fire</groupId>
  <artifactId>fire-spark_${spark.reference}</artifactId>
  <version>${fire.version}</version>
</dependency>
<dependency>
  <groupId>com.zto.fire</groupId>
  <artifactId>fire-enhance-spark_${spark.reference}</artifactId>
  <version>${fire.version}</version>
</dependency>
<dependency>
  <groupId>com.zto.fire</groupId>
  <artifactId>fire-connector-spark-rocketmq_${spark.reference}</artifactId>
  <version>${fire.version}</version>
</dependency>
<dependency>
  <groupId>com.zto.fire</groupId>
  <artifactId>fire-connector-spark-hbase_${spark.reference}</artifactId>
  <version>${fire.version}</version>
</dependency>
<dependency>
  <groupId>com.zto.fire</groupId>
  <artifactId>fire-connector-hbase_${scala.binary.version}</artifactId>
  <version>${fire.version}</version>
</dependency>
<dependency>
  <groupId>com.zto.fire</groupId>
  <artifactId>fire-connector-jdbc_${scala.binary.version}</artifactId>
  <version>${fire.version}</version>
</dependency>

<!-- flink任务单独引入fire-flink依赖 -->
<dependency>
  <groupId>com.zto.fire</groupId>
  <artifactId>fire-flink_${flink.reference}</artifactId>
  <version>${fire.version}</version>
</dependency>
<dependency>
  <groupId>com.zto.fire</groupId>
  <artifactId>fire-enhance-flink_${flink.reference}</artifactId>
  <version>${fire.version}</version>
</dependency>
<dependency>
  <groupId>com.zto.fire</groupId>
  <artifactId>fire-enhance-arthas_${scala.binary.version}</artifactId>
  <version>${fire.version}</version>
</dependency>
<dependency>
  <groupId>com.zto.fire</groupId>
  <artifactId>fire-connector-flink-clickhouse_${flink.reference}</artifactId>
  <version>${fire.version}</version>
</dependency>
<dependency>
  <groupId>com.zto.fire</groupId>
  <artifactId>fire-connector-hbase_${scala.binary.version}</artifactId>
  <version>${fire.version}</version>
</dependency>
<dependency>
  <groupId>com.zto.fire</groupId>
  <artifactId>fire-connector-jdbc_${scala.binary.version}</artifactId>
  <version>${fire.version}</version>
</dependency>
<dependency>
  <groupId>com.zto.fire</groupId>
  <artifactId>fire-connector-flink-rocketmq_${flink.reference}</artifactId>
  <version>${fire.version}</version>
</dependency>
<dependency>
  <groupId>com.zto.fire</groupId>
  <artifactId>fire-metrics_${scala.binary.version}</artifactId>
  <version>${fire.version}</version>
</dependency>
```

### 示例pom.xml

- [spark项目](pom/spark-pom.xml)
- [flink项目](pom/flink-pom.xml)

