package com.zto.fire.examples.flink

import com.zto.fire._
import com.zto.fire.flink.BaseFlinkStreaming
import com.zto.fire.flink.sql.FlinkSqlParser
import org.apache.flink.table.api.SqlDialect
import org.apache.flink.table.catalog.ObjectPath

object FlinkSQLParseTest extends BaseFlinkStreaming {


  override def process: Unit = {
    val select = "select t1.id,t1.name from test t1 where t1.a > 1"
    val selectJoin = "select t1.id,t2.name from tmp.test t1 left join ods.t_user t2 right join dim.baseuser t3 on t1.id=t3.id where t1.a > 1"
    val insertInto = s"insert into sink ${selectJoin}"
    val insertOverwrite = "insert overwrite dw.kwang_test partition(ds='202106', city='beijing') values(4,'zz')"
    val createView = s"create view t_view as ${selectJoin}"
    val createTable = "CREATE TABLE wjk_sink(id int,code String,PRIMARY KEY (id, code) NOT ENFORCED) WITH( 'password'='ZTOzto123!@#','connector'='jdbc','driver'='com.mysql.jdbc.Driver','table-name'='zwp_test','url'='jdbc:mysql://10.9.46.107:3306/test?useSSL=false','username'='root')"
    val createTableAsSelect = s"CREATE TABLE t_baseuser like tmp.test"

    val alterTableAddPartitionStatement =
      """
        |alter table tmp.t_user add partition (ds='20210620', city = 'beijing')
        |""".stripMargin
    val dropTable =
      """
        |drop table if exists tmp.test
        |""".stripMargin
    val renameTable =
      """
        |alter table tmp.t_user rename to ods.t_user2
        |""".stripMargin
    val dropPartition =
      """
        |ALTER TABLE tmp.food DROP PARTITION (ds='20151219', city = 'beijing')
        |""".stripMargin
    val dropDB = "drop database tmp"
    /*this.sqlParser(select)
    println("===================")*/
    // FlinkSqlParser.sqlParser(alterTableAddPartitionStatement)
    // this.tableEnv.getConfig.setSqlDialect(SqlDialect.HIVE)
    // FlinkSqlParser.tableSet.foreach(println)
    // this.fire.sql("select * from hive.dim.baseorganize limit 10").print()
    FlinkSqlParser.sqlParser(
      """
        |CREATE TABLE t_student (
        |  `table` STRING,
        |  `before` ROW(id bigint, age int, name string, length double, createTime string),
        |  `after` ROW(id bigint, age int, name string, length double, createTime string)
        |) WITH (
        |  'connector' = 'kafka',
        |  'topic' = 'fire',
        |  'properties.bootstrap.servers' = '10.9.46.111:9092',
        |  'properties.group.id' = 'fire',
        |  'scan.startup.mode' = 'earliest-offset',
        |  'format' = 'json'
        |)
        |""".stripMargin)
    FlinkSqlParser.sqlParser("select * from tmp.baseorganize")
    FlinkSqlParser.tableMap.foreach(println)
  }

}
