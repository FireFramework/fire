package com.zto.fire.examples.spark

import com.zto.fire.common.anno.Config
import com.zto.fire.examples.bean.Student
import com.zto.fire.spark.BaseSparkCore
import com.zto.fire.spark.sql.SparkSqlParser

/**
 * Spark SQL血缘解析工具
 */
@Config("hive.cluster=test")
object SparkSqlParseTest extends BaseSparkCore {


  override def process: Unit = {
    val ds = this.spark.createDataFrame(Student.newStudentList(), classOf[Student])
    ds.createOrReplaceTempView("t_student")
    println("t_student -> " + SparkSqlParser.isHiveTable(null, "t_student"))
    println("tmp.baseuser ->" + SparkSqlParser.isHiveTable("tmp", "baseuser"))

    val select2 =
      """
        |select bill_event_id,count(*) from hudi.hudi_bill_item group by bill_event_id
        |""".stripMargin
    val select1 =
      """
        |select count(*)
        |from (select * from st.st_fwzl_transfer_kpi_detail_month) a
        |left join (select biz_no,bill_code from dw.dw_kf_center_to_center_dispatch_delay where ds>='20210101') b
        |on a.bill_code=b.bill_code
        |""".stripMargin
    val insertInto =
      """
        |insert into ods.base select a,v from tmp.t_user t1 left join ods.test t2 on t1.id=t2.id
        |""".stripMargin
    val alterTableAddPartitionStatement =
      """
        |alter table tmp.t_user add if not exists partition (ds='20210620', city = 'beijing')
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
        |ALTER TABLE tmp.food DROP IF EXISTS PARTITION (ds='20151219', city = 'beijing')
        |""".stripMargin
    val renamePartition =
      """
        |Alter table tmp.test partition (ds='201801', city='beijing') rename to partition(ds='202106', city='shanghai')
        |""".stripMargin
    val createTable =
      """
        |CREATE TABLE `tmp.test`(
        |  `dept_no` int,
        |  `addr` string,
        |  `tel` string)
        |partitioned by(ds string, city string)
        |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        |""".stripMargin
    val dropDB = "drop database tmp"
    val insertOverwrite = "insert overwrite table dw.kwang_test partition(ds='202106', city='beijing') values(4,'zz')"
    SparkSqlParser.sqlParser(insertInto)
    /*SparkSqlParser.sqlParser(dropTable)
    SparkSqlParser.sqlParser(createTable)
    SparkSqlParser.sqlParser(dropDB)
    SparkSqlParser.sqlParser(renameTable)*/
  }
}
