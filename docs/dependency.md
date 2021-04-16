### 依赖管理

```xml
<dependency>
	<groupId>com.zto.fire</groupId>
	<artifactId>fire-common_${scala.binary.version}</artifactId>
	<version>${project.version}</version>
</dependency>
<dependency>
	<groupId>com.zto.fire</groupId>
	<artifactId>fire-core_${scala.binary.version}</artifactId>
	<version>${project.version}</version>
</dependency>
<dependency>
	<groupId>com.zto.fire</groupId>
	<artifactId>fire-jdbc_${scala.binary.version}</artifactId>
	<version>${project.version}</version>
</dependency>
<dependency>
	<groupId>com.zto.fire</groupId>
	<artifactId>fire-hbase_${scala.binary.version}</artifactId>
	<version>${project.version}</version>
</dependency>
<!-- spark任务单独引入fire-spark依赖 -->
<dependency>
    <groupId>com.zto.fire</groupId>
    <artifactId>fire-spark_${spark.reference}</artifactId>
    <version>${project.version}</version>
</dependency>
<!-- flink任务单独引入fire-flink依赖 -->
<dependency>
    <groupId>com.zto.fire</groupId>
    <artifactId>fire-flink_${flink.reference}</artifactId>
    <version>${project.version}</version>
</dependency>
```

### 示例pom.xml

- [spark项目](pom/spark-pom.xml)
- [flink项目](pom/flink-pom.xml)

