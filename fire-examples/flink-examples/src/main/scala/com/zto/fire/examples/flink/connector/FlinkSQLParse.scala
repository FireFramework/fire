package com.zto.fire.examples.flink.connector

import com.zto.fire.examples.flink.connector.FlinkSqlCommit.getSqlParse
import org.apache.calcite.avatica.util.Quoting
import org.apache.calcite.avatica.util.Casing
import org.apache.flink.sql.parser.ddl.SqlCreateView
import org.apache.calcite.sql.{SqlIdentifier, SqlNode, SqlNodeList, SqlSelect}
import org.apache.calcite.sql.parser.SqlParser
import org.apache.flink.sql.parser.dml.RichSqlInsert
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl
import org.apache.flink.sql.parser.validate.FlinkSqlConformance
import org.apache.flink.table.api.SqlDialect

object FlinkSQLParseTest {

  def getSqlParse(sql:String): SqlParser={
    val parser = SqlParser.create(sql,
      SqlParser.configBuilder.setParserFactory(
        FlinkSqlParserImpl.FACTORY).
        setQuoting(Quoting.BACK_TICK).
        setUnquotedCasing(Casing.TO_UPPER).
        setQuotedCasing(Casing.UNCHANGED).
        setConformance(FlinkSqlConformance.DEFAULT).
        build
    )
    parser
  }

  @throws[Exception]
  def parseSql(sql: String): Unit = {
    val sqlNode: SqlNode = getSqlParse(sql).parseStmt()
    println(sqlNode)

  }

  @throws[Exception]
  def parseInsertSql(sql: String): Unit = {

      val sqlNodeList: SqlNodeList = getSqlParse(sql).parseStmtList
      val sqlInto: RichSqlInsert = sqlNodeList.getList.get(0).asInstanceOf[RichSqlInsert]
      val names =  sqlInto.getTargetTable.asInstanceOf[SqlIdentifier].names
      println(sqlInto)

  }

  @throws[Exception]
  def parseSelectSql(sql: String): Unit = {
    val sqlNodeList: SqlNodeList = getSqlParse(sql).parseStmtList
    var sqlSelect: SqlSelect = sqlNodeList.getList.get(0).asInstanceOf[SqlSelect]
    println("from table:" + sqlSelect.getFrom)
    println("where:" + sqlSelect.getWhere)
    println("select list:" + sqlSelect.getSelectList)

  }

  @throws[Exception]
  def parseViewSql(sql: String): Unit = {
    var sqlNodeList: SqlNodeList = getSqlParse(sql).parseStmtList
    var sqlSelect: SqlCreateView = sqlNodeList.getList.get(0).asInstanceOf[SqlCreateView]
    println("parseViewSql:" + sqlSelect)
    println("view table:" + sqlSelect.getViewName)

  }


  def main(args: Array[String]): Unit = {
    //parseSql("select * from test where a > 1");
    //parseSql("CREATE TABLE wjk_sink (id int,code String,PRIMARY KEY (id, code) NOT ENFORCED) WITH( 'password'='ZTOzto123!@#','connector'='jdbc','driver'='com.mysql.jdbc.Driver','table-name'='zwp_test','url'='jdbc:mysql://10.9.46.107:3306/test?useSSL=false','username'='root')")
    //parseSql("CREATE TABLE wjk_sink (id int PRIMARY KEY NOT ENFORCED,code String) WITH( 'password'='ZTOzto123!@#','connector'='jdbc','driver'='com.mysql.jdbc.Driver','table-name'='zwp_test','url'='jdbc:mysql://10.9.46.107:3306/test?useSSL=false','username'='root')")
    //parseSql("CREATE TABLE Orders_with_watermark(`user` BIGINT,product STRING,order_time TIMESTAMP(3),PRIMARY KEY (`user`),WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND ) WITH ('connector' = 'kafka','scan.startup.mode' = 'latest-offset')")
    //parseSql("CREATE TABLE demo_kafka_20210513_162843_tmp (demo_String STRING ,demo_Time_Stamp TIMESTAMP ,demo_boolean_ture BOOLEAN ,demo_boolean_false BOOLEAN ,demo_null STRING ,demo_empty STRING ,demo_b STRING ,demo_f STRING ,demo_n STRING ,demo_r STRING ,demo_t STRING ,demo_Integer_three INTEGER ,demo_Integer_four BIGINT ,demo_Floating_Point_Number DECIMAL ,demo_Object_Key_One_1 ROW<demo_Object_Key_Two_1 STRING,demo_Object_Key_Two_2 STRING,demo_Object_Key_Two_3 ROW<demo_Object_Key_Three_1 STRING,demo_Object_Key_Three_2 STRING,demo_Object_Key_Three_3 ROW<demo_Object_Key_Four_1 STRING,demo_Object_Key_Four_2 STRING,demo_Object_Key_Four_3 ROW<demo_Object_Key_Five_1 STRING,demo_Object_Key_Five_2 STRING,demo_Object_Key_Five_3 STRING>>>> ,demo_Object_Key_One_2 ROW<demo_Object_Key_Two_21 STRING,demo_Object_Key_Two_22 STRING,demo_Object_Key_Two_23 STRING> ,demo_Array_One_1 ARRAY<STRING> ,demo_Array_One_2 ARRAY<ROW<demo_Array_Object_Key_Two_2_11 STRING,demo_Array_Object_Key_Two_2_12 STRING,demo_Array_Object_Key_Two_2_13 STRING>> ,demo_Array_One_3 ARRAY<ROW<demo_Array_Object_Key_Two_3_31 STRING,demo_Array_Object_Key_Two_3_32 STRING,arr1 ARRAY<ROW<demo_Array_Object_Key_Three_31 STRING,demo_Array_Object_Key_Three_32 STRING,arr3 ARRAY<ROW<demo_Array_Object_Key_Four_31 STRING,demo_Array_Object_Key_Four_32 STRING,arr2 ARRAY<ROW<demo_Array_Object_Key_Five_31 STRING,demo_Array_Object_Key_Five_32 STRING,arr4 ARRAY<ROW<demo_Array_Object_Key_Six_31 STRING,demo_Array_Object_Key_Six_32 STRING,demo_Array_Object_Key_Six_33 STRING>>>>>>>>>> ) WITH( 'properties.bootstrap.servers'='10.9.46.111:9092','connector'='kafka','format'='json','topic'='20210513_162843','properties.group.id'='zrc_112','scan.startup.mode'='earliest-offset')")
    //parseSql("insert into aa.demo_mysql_sink_tmp select demo_kafka16_mysql_INTtest,demo_kafka17_mysql_TINYINTtest,demo_kafka18_mysql_SMALLINTtest,demo_kafka19_mysql_MEDIUMINTtest,demo_kafka20_mysql_BIGINTtest,demo_kafka21_mysql_FLOATtest,demo_kafka22_mysql_DOUBLEtest,demo_kafka23_mysql_DECIMALtest,demo_kafka24_mysql_DATEtest,demo_kafka25_mysql_TIMEtest,demo_kafka26_mysql_DATETIMEtest,demo_kafka27_mysql_TIMESTAMPtest,demo_kafka28_mysql_YEARtest,demo_kafka29_mysql_CHARtest,demo_kafka30_mysql_VARCHARtest,demo_kafka31_mysql_TEXTtest,demo_kafka32_mysql_TINYTEXTtest,demo_kafka33_mysql_MEDIUMTEXTtest,demo_kafka34_mysql_LONGTEXTtest from demo_kafka_20210513_162843_tmp where demo_kafka11_String = 'demo_kafka_String_20210517_090934'")
    parseViewSql("create VIEW table_a as select demo_String from demo_kafka_20210513_162843_tmp where demo_String = 'demo_String_20210513_162843' Limit 10")
  }

}
