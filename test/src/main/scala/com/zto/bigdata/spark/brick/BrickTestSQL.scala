package com.zto.bigdata.spark.brick

/**
  * sql语句
  */
object BrickTestSQL {

  /**
    * 同一个id，取version最大的记录
    *
    * @param tableName
    * 临时表名
    * @return
    * sql语句
    */
  def mergeByVersion(tableName: String): String = {
    s"""
       |  select t.*
       |   from (select t1.*,
       |                row_number() over(partition by t1.id order by t1.version desc) as num
       |           from $tableName t1) t
       |  where t.num = 1
    """.stripMargin
  }

  /**
    * item表字段列表
    * @return
    */
  def itemFields(tableName: String): String = {
    s"""
      |select id,bill_event_id,bill_code,bill_code_hash,bill_type,bill_sub_type,rec_site_id,rec_site_name,rec_site_province_code,rec_site_city_id,rec_site_county_id,rec_man_id,rec_man_name,customer_id,customer_code,customer_name,disp_site_id,disp_site_name,disp_site_province_code,disp_site_city_id,disp_site_county_id,disp_man_id,disp_man_name,standard_weight,charge_weight,bill_sum_time,receiver_id,receiver_name,payer_id,payer_name,balance_no,biz_balance_no,fee,bl_adjust,bl_has_redback,redback_id,status,version,create_time,modify_time,modify_by_id,modify_by_name,modify_by_site_id,modify_by_site_name,policy_code,receiver_flow_no,payer_flow_no,rec_site_city_name,disp_site_city_name from $tableName
    """.stripMargin
  }

  /**
    * event表字段列表
    * @return
    */
  def eventFields(tableName: String): String = {
    s"""
      |select id,bill_code,bill_code_hash,bill_sub_type,rec_site_id,rec_site_name,rec_site_province_code,rec_site_city_id,rec_site_county_id,rec_man_id,rec_man_name,customer_id,customer_code,customer_name,disp_site_id,disp_site_name,disp_site_province_code,disp_site_city_id,disp_site_county_id,disp_man_id,disp_man_name,bl_sign,standard_weight,charge_weight,remark,status,details_status,version,create_time,modify_time,modify_by_id,modify_by_name,modify_by_site_id,modify_by_site_name,abnormal_trace,fee,rec_site_city_name,disp_site_city_name from $tableName
    """.stripMargin
  }
}
