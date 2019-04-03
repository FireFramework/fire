package com.zto.bigdata.spark.tidb

object TISql {

  def sql: String = {
      """
        |select
        |      a.assign_site_code site_id,
        |    DATE_FORMAT(a.order_create_date,'%Y-%m-%d') scan_day,
        |    a.cust_code,
        |    a.cust_name,
        |    count(1) OrderCnt,
        |    sum(case when (a.REC_SITE_DATE is not null and a.REC_SITE_CODE is not null) then 1 else 0 end) RecCnt
        |    from rtdb.zto_order_detail a
        |      where a.assign_site_code is not null
        |    and a.assign_site_code != ''
        |    and a.cust_code is not null
        |    and a.cust_name is not null
        |    and a.order_create_date >='2019-04-02'
        |    group by
        |      a.assign_site_code,
        |    a.cust_code,
        |    a.cust_name,
        |    DATE_FORMAT(a.order_create_date,'%Y-%m-%d')
      """.stripMargin
  }
}
