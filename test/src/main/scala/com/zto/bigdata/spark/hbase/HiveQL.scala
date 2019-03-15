package com.zto.bigdata.spark.hbase

/**
  * Hive sql
  * @author ChengLong 2019-1-16 09:53:45
  */
object HiveQL {

  /**
    * 执行order main sql
    * @param tableName
    * @return
    */
  def saveMainOrder(tableName: String): String = {
    s"""
       |select
       |gtid,
       |logFile,
       |offset,
       |op_type,
       |pos,
       |schema,
       |table,
       |msg_when,
       |after.*,
       |before.bill_code before_bill_code,
       |before.order_code before_order_code
       |from ${tableName}
       |where op_type<>'D'
       |and after.bill_code<>''
       |and substr(table,0,6)='order_'
       |and substr(table,0,7)<>'order_r'
      """.stripMargin
  }

  /**
    * 执行delete order main sql
    * @param tableName
    * @return
    */
  def deleteMainOrder(tableName: String): String = {
    s"""
       |select
       |gtid,
       |logFile,
       |offset,
       |op_type,
       |pos,
       |schema,
       |table,
       |msg_when,
       |before.*
       |from ${tableName}
       |where op_type='D'
       |and before.bill_code<>''
       |and before.order_create_date>'2018-06-01'
       |and substr(table,0,6)='order_'
       |and substr(table,0,7)<>'order_r'
      """.stripMargin
  }

  /**
    * 执行save replica order sql
    * @param tableName
    * @return
    */
  def saveReplicaOrder(tableName: String): String = {
    s"""
       |select
       |gtid,
       |logFile,
       |offset,
       |op_type,
       |pos,
       |schema,
       |table,
       |msg_when,
       |after.*,
       |before.bill_code before_bill_code,
       |before.order_code before_order_code
       |from ${tableName}
       |where op_type<>'D'
       |and after.bill_code<>''
       |and substr(table,0,7)='order_r'
      """.stripMargin
  }

  /**
    * 执行delete replica order sql
    * @param tableName
    * @return
    */
  def deleteReplicaOrder(tableName: String): String = {
    s"""
       |select
       |gtid,
       |logFile,
       |offset,
       |op_type,
       |pos,
       |schema,
       |table,
       |msg_when,
       |before.*
       |from ${tableName}
       |where op_type='D'
       |and before.order_create_date>'2018-06-01'
       |and before.bill_code<>''
       |and substr(table,0,7)='order_r'
      """.stripMargin
  }
}
