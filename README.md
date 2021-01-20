# Fire框架
---
Fire框架是由中通科技开源的，专门用于大数据实时计算领域的开发框架。该框架具有易学易用，稳定可靠等诸多优点，陪伴着中通度过了一个又一个双11。
## 现状
基于Fire框架的任务在中通每天处理的数据量高达千亿级别以上，覆盖了Spark离线任务、Spark实时任务与Flink任务等众多场景，任务覆盖率高达98%以上。
## 赋能开发者
Fire框架自研发之日起就以简单高效、稳定可靠为目标。通过封装技术细节、提供通用简洁的API这种方式，将开发者从技术的大海中拯救出来，让开发者更专注于业务代码开发。Fire框架支持Spark与Flink两大引擎，并且覆盖离线计算与实时实时两大场景，内部提供了丰富的API，许多操作仅需一行代码，大大提升了生产力。
### HBase 操作：
`// 以多版本方式get，并将结果集封装到rdd中返回
  val studentRDD = this.fire.hbaseGetRDD(this.tableName1, classOf[Student], getRDD)
  // get到的结果以dataframe形式返回
  val studentDF = this.fire.hbaseGetDF(this.tableName1, classOf[Student], getRDD)`
### JDBC 操作：
`val insertSql = s"INSERT INTO spark_test(name, age, createTime, length, sex) VALUES (?, ?, ?, ?, ?)"
 // 指定部分DataFrame列名作为参数，顺序要对应sql中问号占位符的顺序，batch用于指定批次大小，默认取spark.db.jdbc.batch.size配置的值
 df.jdbcBatchUpdate(insertSql, Seq("name", "age", "createTime", "length", "sex"), batch = 100)`
可以看到，Fire框架中的API是以DataFrame、RDD为中心进行了高度抽象，通过引入fire的隐式转换，让RDD、DataFrame等对象直接具有了这些API的能力，进而实现直接调用。
## 赋能平台
Fire框架可以将实时任务与实时管理平台进行绑定，实现很多酷炫又实用的功能。比如配置管理、SQL在线调试、任务热重启、配置热更新等，甚至可以直接获取到任务的运行时数据，实现更细粒度的监控管理。
配置管理
类似于携程的apollo，实时任务管理平台可提供任务配置的管理功能，基于Fire的实时任务在启动时会主动拉取配置信息，并覆盖任务jar包中的配置文件，避免重复打包发布。
SQL在线调试
基于该技术，可以在实时任务管理平台中提交SQL语句，交由指定的Spark Streaming任务执行，并将结果返回，该功能的好处是支持Spark内存临时表，便于在web端进行Spark SQL的调试，大幅节省SQL开发时间。
任务热重启
该功能是针对Spark Streaming任务的，通过该功能，可以在不重启Spark Streaming任务的前提下，实现批次时间的热修改。比如在web端将某个任务的批次时间调整为10s。
配置热更新
用户仅需在web页面中更新指定的配置信息，就可以让实时任务接收到最新的配置。最典型的应用场景是进行Spark Streaming repartition操作的更新，比如当任务处理的数据量较大时，可以通过该功能将repartition的具体分区数修改掉。
运行时信息
当进行HBase或MySQL维护时，可能会重点关注使用了这几个组件的任务。基于Fire框架，可在运行时分析哪些任务读写了HBase，哪些任务操作了MySQL数据库。

## 程序结构
###Spark
`/**
  * 基于Fire进行Spark Streaming开发
  */
 object Test extends BaseSparkStreaming {
 
   override def process: Unit = {
     val dstream = this.fire.createKafkaDirectStream()
     dstream.print
     this.fire.start
   }
 
 
   def main(args: Array[String]): Unit = {
     this.init(10, false)
   }
 }
`
###Flink
`/**
  * Flink流式计算任务模板
  *
  * @author ChengLong
  * @since 1.0.0
  * @create 2021-01-18 17:24
  */
 class Test extends BaseFlinkStreaming {
 
   override def process: Unit = {
     val dstream = this.fire.createKafkaDirectStream()
     dstream.print
     this.fire.start
   }
 
   def main(args: Array[String]): Unit = {
     this.init()
   }
 }
`

