package com.zto.fire.demo.spark

import com.zto.fire.core.util.FireUtils
import com.zto.fire.core.{BaseSparkCore, BaseSparkStreaming}

object Test extends BaseSparkStreaming {

  override def process: Unit = {
    val start = System.currentTimeMillis()
    this.spark.sql(
      """
        |SELECT
        |	a.order_date,
        |	a.site_id,
        |	a.cust_code,
        |	a.cust_name,
        |	a.bill_type,
        |	a.sum_day,
        |	a.statis_type,
        |  a.time_range,
        |  sum(a.should_rec_cnt_18_18) as should_rec_cnt_18_18,
        |	sum(a.order_num_0_18) AS order_num_0_18,
        |	sum(a.order_num_18_0) AS order_num_18_0,
        |  sum(a.had_rec_cnt_18_18) as had_rec_cnt_18_18,
        |  sum(a.in_time_rec_cnt_18_18) as in_time_rec_cnt_18_18,
        |  sum(a.not_rec_cnt_18_18) as not_rec_cnt_18_18,
        |	sum(a.rec_num_0_18) AS rec_num_0_18,
        |	sum(a.rec_num_18_0) AS rec_num_18_0,
        |  sum(a.not_rec_num_0_18) as not_rec_num_0_18,
        |  sum(a.not_rec_num_18_0) as not_rec_num_18_0,
        |	sum(a.order_num) AS order_num,
        |  sum(a.send_rec_cnt) as send_rec_cnt,
        |  sum(a.in_transit_center_cnt) as in_transit_center_cnt,
        |  sum(a.in_time_hand_cnt) as in_time_hand_cnt,
        |  sum(a.send_rec_cnt-a.in_time_hand_cnt) as over_time_hand_cnt,
        |  sum(a.should_rec_cnt_18_18-a.had_rec_cnt_18_18) as over_time_rec_cnt_18_18,
        |  sum(a.not_send_cnt) as not_send_cnt,
        |  sum(a.in_center_not_come_cnt) as in_center_not_come_cnt,
        |  sum(a.district_time_after_cnt) as district_time_after_cnt,
        |	sum(a.not_send_0_18) AS not_send_0_18,
        |	sum(a.not_send_18_0) AS not_send_18_0,
        |	sum(a.in_time_rec_0_18) AS in_time_rec_0_18,
        |	sum(a.in_time_rec_18_0) AS in_time_rec_18_0,
        |  sum(a.no_in_time_rec_0_18)  as no_in_time_rec_0_18,
        |  sum(a.no_in_time_rec_18_0)  as no_in_time_rec_18_0,
        |	sum(a.rec_num) AS rec_num,
        |  --新增订单分时量，积压揽收量
        |  sum(a.range_order_num) as range_order_num,
        |  sum(a.over_rec_num) as over_rec_num,
        |  sum(a.range_rec_num) as range_rec_num,
        |  sum(a.over_send_num) as over_send_num,
        |  sum(a.range_send_num) as range_send_num,
        |	sum(a.send_num) AS send_num,
        |	sum(a.not_send_num) AS not_send_num,
        |	sum(a.sign_num) AS sign_num,
        |	sum(a.op_sign_num) AS op_sign_num,
        |  sum(a.range_sign_num) as range_sign_num,
        |  sum(a.over_sign_num) as over_sign_num,
        |	sum(a.op_disp_num) AS op_disp_num,
        |  sum(a.range_disp_num) as range_disp_num,
        |  sum(a.over_disp_num) as over_disp_num,
        |	sum(a.send_num_12) AS send_num_12,
        |	sum(a.send_num_24) AS send_num_24,
        |	sum(a.send_num_48) AS send_num_48,
        |	sum(a.send_num_48_hup) AS send_num_48_hup,
        |	sum(a.sign_num_24) AS sign_num_24,
        |	sum(a.sign_num_48) AS sign_num_48,
        |	sum(a.sign_num_3d) AS sign_num_3d,
        |	sum(a.sign_num_4d) AS sign_num_4d,
        |	sum(a.sign_num_5d) AS sign_num_5d,
        |	sum(a.sign_num_10d) AS sign_num_10d,
        |	sum(a.in_time_rec_0_24)  AS in_time_rec_0_24,
        |  sum(a.no_in_time_rec_0_24) AS no_in_time_rec_0_24,
        |  sum(a.until_now_no_rec_0_24) AS until_now_no_rec_0_24,
        |  sum(a.lack_rec_0_24) AS lack_rec_0_24,
        |  sum(a.out_rec_0_24) AS out_rec_0_24
        |FROM
        |	(
        |  select
        |    --做了18点到0 0点到18点的区分取的订单时间
        |    a.scan_day as order_date,
        |    --订单统一用forecast_first_site_id
        |    a.forecast_rec_site_id as site_id,
        |    a.cust_code,
        |    a.cust_name,
        |    a.channel_code as bill_type,
        |    a.scan_day as sum_day,
        |    0 as statis_type,
        |    0 as time_range,
        |    --should_rec_tb_cnt 这个去掉  should_rec_cnt_18_18 = should_rec_cnt
        |    sum(1) as should_rec_cnt_18_18,
        |    sum(case when date_flag = 1 then 1 else 0 end) as order_num_0_18,
        |    sum(case when date_flag = 0 then 1 else 0 end) as order_num_18_0,
        |    --改成 had_rec_cnt 去掉 should_rec_tb_cnt 统一统计 had_rec_cnt_18_18 = had_rec_cnt
        |    sum(case when (a.rec_site_id > 0) then 1 else 0 end) as had_rec_cnt_18_18,
        |    --  in_time_rec_tb_cnt 这个去掉  in_time_rec_cnt_18_18 = in_time_rec_cnt
        |    sum(case when (a.rec_site_id > 0 and a.rec_date <= a.deadline_rec_date) then 1 else 0 end) in_time_rec_cnt_18_18,
        |    -- not_rec_cnt_18_18 = not_rec_cnt
        |    sum(case when (a.rec_site_id = 0 and a.first_center_id = 0 and a.sign_site_id = 0 and a.dpm_site_id = 0 and a.disp_site_id = 0 and a.last_center_id = 0 ) then 1 else 0 end) not_rec_cnt_18_18,
        |    0 as rec_num_0_18,
        |    0 as rec_num_18_0,
        |    0 as not_rec_num_0_18,
        |    0 as not_rec_num_18_0,
        |    0 as order_num,
        |    --新增
        |    0 as send_rec_cnt,
        |    0 as in_transit_center_cnt,
        |    0 as in_time_hand_cnt,
        |    0 as not_send_cnt,
        |    0 as in_center_not_come_cnt,
        |    0 as district_time_after_cnt,
        |    0 as not_send_0_18,
        |    0 as not_send_18_0,
        |    0 as in_time_rec_0_18,
        |    0 as in_time_rec_18_0,
        |    0 as no_in_time_rec_0_18,
        |    0 as no_in_time_rec_18_0,
        |	  0 as rec_num,
        |    0 as range_order_num,
        |    0 as over_rec_num,
        |    0 as range_rec_num,
        |    0 as over_send_num,
        |    0 as range_send_num,
        |	  0 as send_num,
        |    0 as not_send_num,
        |    0 as sign_num,
        |    0 as op_sign_num,
        |    0 as range_sign_num,
        |    0 as over_sign_num,
        |    0 as op_disp_num,
        |    0 as range_disp_num,
        |    0 as over_disp_num,
        |	  0 as send_num_12,
        |	  0 as send_num_24,
        |	  0 as send_num_48,
        |	  0 as send_num_48_hup,
        |	  0 as sign_num_24,
        |	  0 as sign_num_48,
        |	  0 as sign_num_3d,
        |	  0 as sign_num_4d,
        |	  0 as sign_num_5d,
        |	  0 as sign_num_10d,
        |	  0 as in_time_rec_0_24,
        |      0 as no_in_time_rec_0_24,
        |      0 as until_now_no_rec_0_24,
        |      0 as lack_rec_0_24,
        |      0 as out_rec_0_24
        |from tmp.tmp_time_order_monitor_dig_source_table_0514 a
        |where a.order_create_date > '1999-01-01 00:00:00'
        |group by
        |    a.scan_day,
        |    a.forecast_rec_site_id,
        |    a.cust_code,
        |    a.cust_name,
        |    a.channel_code
        |union all
        |select
        |    --做了18点到0 0点到18点的区分取的订单时间
        |    a.scan_day as order_date,
        |    --订单统一用forecast_first_site_id
        |    a.rec_site_id as site_id,
        |    a.cust_code,
        |    a.cust_name,
        |    a.channel_code as bill_type,
        |    substr(a.rec_date,0,10) as sum_day,
        |    0 as statis_type,
        |    0 as time_range,
        |    --新增
        |    0 as should_rec_cnt_18_18,
        |    0 as order_num_0_18,
        |    0 as order_num_18_0,
        |    0 as had_rec_cnt_18_18,
        |    0 as in_time_rec_cnt_18_18,
        |    0 as not_rec_cnt_18_18,
        |    sum(case when (date_flag = 1 and rec_site_id > 0) then 1 else 0 end) as rec_num_0_18,
        |    sum(case when (date_flag = 0 and rec_site_id > 0) then 1 else 0 end) as rec_num_18_0,
        |    sum(case when (date_flag = 1 and rec_site_id = 0 and first_center_id = 0 and sign_site_id = 0 and dpm_site_id = 0 and dpm_site_id = 0 and last_center_id = 0 ) then 1 else 0 end) as not_rec_num_0_18,
        |    sum(case when (date_flag = 0 and rec_site_id = 0 and first_center_id = 0 and sign_site_id = 0 and dpm_site_id = 0 and dpm_site_id = 0 and last_center_id = 0) then 1 else 0 end) as not_rec_num_18_0,
        |    0 as order_num,
        |    --send_rec_cnt = send_rec_cnt
        |    sum(case when (a.rec_site_id > 0) then 1 else 0 end) as send_rec_cnt,
        |    sum(case when (a.rec_site_id > 0 and a.first_center_id > 0 and a.sign_site_id = 0 and a.dpm_site_id = 0 and a.last_center_id = 0 and a.disp_site_id = 0) then 1 else 0 end) as in_transit_center_cnt,
        |    sum(case when (a.rec_site_id > 0 and a.first_center_come_date < a.district_night_date and a.first_center_id > 0 and a.first_center_come_date >'1999-01-01 00:00:00' and
        |    a.district_night_date >'1999-01-01 00:00:00' and a.sign_site_id = 0 and a.dpm_site_id = 0 and a.last_center_id = 0 and a.disp_site_id = 0) then 1 else 0 end) in_time_hand_cnt,
        |    sum(case when (a.rec_site_id > 0 and a.fir_send_center_site_id = 0 and a.sign_site_id = 0 and a.dpm_site_id = 0 and a.disp_site_id = 0 and a.first_center_id = 0 and a.last_center_id = 0 ) then 1 else 0 end) not_send_cnt,
        |    sum(case when (a.rec_site_id > 0 and a.fir_send_center_site_id > 0 and a.first_center_id = 0 and a.sign_site_id = 0 and a.dpm_site_id = 0 and a.last_center_id = 0 and a.disp_site_id = 0 ) then 1 else 0 end) in_center_not_come_cnt,
        |    sum(case when (a.rec_site_id > 0 and a.first_center_come_date >= a.district_night_date and a.first_center_id > 0 and a.first_center_come_date >'1999-01-01 00:00:00' and
        |    a.district_night_date >'1999-01-01 00:00:00' and a.sign_site_id = 0 and a.dpm_site_id = 0 and a.last_center_id = 0 and a.disp_site_id = 0 ) then 1 else 0 end) district_time_after_cnt,
        |    sum(case when (date_flag = 1 and rec_site_id > 0 and fir_send_center_site_id = 0 and sign_site_id = 0 and dpm_site_id = 0 and disp_site_id = 0 and first_center_id = 0 and last_center_id = 0 ) then 1 else 0 end ) as not_send_0_18,
        |    sum(case when (date_flag = 0 and rec_site_id > 0 and fir_send_center_site_id = 0 and sign_site_id = 0 and dpm_site_id = 0 and disp_site_id = 0 and first_center_id = 0 and last_center_id = 0 ) then 1 else 0 end ) as not_send_18_0,
        |    sum(case when (date_flag = 1 and rec_site_id > 0 and rec_date <= deadline_rec_date) then 1 else 0 end) as in_time_rec_0_18,
        |    sum(case when (date_flag = 0 and rec_site_id > 0 and rec_date <= deadline_rec_date) then 1 else 0 end) as in_time_rec_18_0,
        |    sum(
        |	   CASE
        |	   WHEN (
        |	   	date_flag = 1
        |	   	AND (
        |	   		(
        |	   			rec_site_id > 0
        |	   			AND rec_date > deadline_rec_date
        |	   		)
        |	   		OR (
        |	   			a.rec_site_id = 0
        |	   			AND a.disp_site_id = 0
        |	   			AND a.dpm_site_id = 0
        |	   			AND a.first_center_id = 0
        |	   			AND a.last_center_id = 0
        |	   			AND from_unixtime(
        |	   				unix_timestamp(),
        |	   				'yyyy-MM-dd HH:mm:ss'
        |	   			) > a.deadline_rec_date
        |	   		)
        |	    	)
        |	   ) THEN
        |	   	1
        |	   ELSE
        |	   	0
        |	   END
        |    ) AS no_in_time_rec_0_18,
        |   sum(
        |	   CASE
        |	   WHEN (
        |	   	date_flag = 0
        |	   	AND (
        |	   		(
        |	   			rec_site_id > 0
        |	   			AND rec_date > deadline_rec_date
        |	   		)
        |	   		OR (
        |	   			a.rec_site_id = 0
        |	   			AND a.disp_site_id = 0
        |	   			AND a.dpm_site_id = 0
        |	   			AND a.first_center_id = 0
        |	   			AND a.last_center_id = 0
        |	   			AND from_unixtime(
        |	   				unix_timestamp(),
        |	   				'yyyy-MM-dd HH:mm:ss'
        |	   			) > a.deadline_rec_date
        |	   		)
        |	    	)
        |	   ) THEN
        |	   	1
        |	   ELSE
        |	   	0
        |	   END
        |    ) AS no_in_time_rec_18_0,
        |	  0 as rec_num,
        |    0 as range_order_num,
        |    0 as over_rec_num,
        |    0 as range_rec_num,
        |    0 as over_send_num,
        |    0 as range_send_num,
        |    0 as send_num,
        |    0 as not_send_num,
        |    0 as sign_num,
        |    0 as op_sign_num,
        |    0 as range_sign_num,
        |    0 as over_sign_num,
        |    0 as op_disp_num,
        |    0 as range_disp_num,
        |    0 as over_disp_num,
        |    0 as send_num_12,
        |    0 as send_num_24,
        |    0 as send_num_48,
        |    0 as send_num_48_hup,
        |    0 as sign_num_24,
        |    0 as sign_num_48,
        |    0 as sign_num_3d,
        |    0 as sign_num_4d,
        |    0 as sign_num_5d,
        |    0 as sign_num_10d,
        |	0 as in_time_rec_0_24,
        |    0 as no_in_time_rec_0_24,
        |    0 as until_now_no_rec_0_24,
        |    0 as lack_rec_0_24,
        |    0 as out_rec_0_24
        |from tmp.tmp_time_order_monitor_dig_source_table_0514 a
        |where  a.order_create_date > '1999-01-01 00:00:00'
        |and  a.rec_date  > '1999-01-01 00:00:00'
        |and a.forecast_rec_site_id > 0
        |group by
        |    a.scan_day,
        |    a.rec_site_id,
        |    a.cust_code,
        |    a.cust_name,
        |    a.channel_code,
        |    substr(a.rec_date,0,10)
        |union all
        |select
        |  substr(a.order_create_date,0,10) as order_date,
        |  --订单用分配forecast_first_site_id
        |  a.forecast_rec_site_id as site_id,
        |  --按照名单发放
        |  a.cust_code,
        |  a.cust_name,
        |  a.channel_code as bill_type,
        |  substr(a.rec_date,0,10) as sum_day,
        |  1 as statis_type,
        |  0 as time_range,
        |  --新增
        |  0 as should_rec_cnt_18_18,
        |  0 as order_num_0_18,
        |  0 as order_num_18_0,
        |  0 as had_rec_cnt_18_18,
        |  0 as in_time_rec_cnt_18_18,
        |  0 as not_rec_cnt_18_18,
        |  0 as rec_num_0_18,
        |  0 as rec_num_18_0,
        |  0 as not_rec_num_0_18,
        |  0 as not_rec_num_18_0,
        |  0 as order_num,
        |  --新增
        |  0 as send_rec_cnt,
        |  0 as in_transit_center_cnt,
        |  0 as in_time_hand_cnt,
        |  0 as not_send_cnt,
        |  0 as in_center_not_come_cnt,
        |  0 as district_time_after_cnt,
        |  0 as not_send_0_18,
        |  0 as not_send_18_0,
        |  0 as in_time_rec_0_18,
        |  0 as in_time_rec_18_0,
        |  0 as no_in_time_rec_0_18,
        |  0 as no_in_time_rec_18_0,
        |  sum(case when (a.rec_site_id >0 or a.sign_site_id > 0 or a.disp_site_id>0 or a.first_center_id>0 or a.last_center_id>0) then 1 else 0 end) as rec_num,
        |  0 as range_order_num,
        |  0 as over_rec_num,
        |  0 as range_rec_num,
        |  0 as over_send_num,
        |  0 as range_send_num,
        |  sum(case when (a.first_center_id>0 or a.last_center_id>0 or a.disp_site_id >0 or a.sign_site_id >0) then 1 else 0 end) as send_num,
        |  0 as not_send_num,
        |  sum(case when (a.sign_site_id >0 or a.dpm_site_id >0 ) then 1 else 0 end) as sign_num,
        |  0 as op_sign_num,
        |  0 as range_sign_num,
        |  0 as over_sign_num,
        |  0 as op_disp_num,
        |  0 as range_disp_num,
        |  0 as over_disp_num,
        |  sum(case when to_unix_timestamp(a.first_center_come_date)-to_unix_timestamp(a.rec_date)<=43200  and  a.first_center_come_date >'1999-01-01 00:00:00' and a.rec_date > '1999-01-01 00:00:00' then 1 else 0 end)  send_num_12,
        |  sum(case when to_unix_timestamp(a.first_center_come_date)-to_unix_timestamp(a.rec_date)<=86400  and  a.first_center_come_date >'1999-01-01 00:00:00' and a.rec_date > '1999-01-01 00:00:00' then 1 else 0 end)  send_num_24,
        |  sum(case when to_unix_timestamp(a.first_center_come_date)-to_unix_timestamp(a.rec_date)<=172800 and  a.first_center_come_date >'1999-01-01 00:00:00' and a.rec_date > '1999-01-01 00:00:00' then 1 else 0 end)  send_num_48,
        |  sum(case when to_unix_timestamp(a.first_center_come_date)-to_unix_timestamp(a.rec_date)>172800  and  a.first_center_come_date >'1999-01-01 00:00:00' and a.rec_date > '1999-01-01 00:00:00' then 1 else 0 end)  send_num_48_hup,
        |  sum(case when to_unix_timestamp(a.sign_date)-to_unix_timestamp(a.rec_date)<=86400 and a.sign_date > '1999-01-01 00:00:00' and a.rec_date > '1999-01-01 00:00:00' then 1 else 0 end)     sign_num_24,
        |  sum(case when to_unix_timestamp(a.sign_date)-to_unix_timestamp(a.rec_date)<=172800 and a.sign_date > '1999-01-01 00:00:00' and a.rec_date > '1999-01-01 00:00:00' then 1 else 0 end)    sign_num_48,
        |  sum(case when to_unix_timestamp(a.sign_date)-to_unix_timestamp(a.rec_date)<=72*3600 and a.sign_date > '1999-01-01 00:00:00' and a.rec_date > '1999-01-01 00:00:00' then 1 else 0 end)   sign_num_3d,
        |  sum(case when to_unix_timestamp(a.sign_date)-to_unix_timestamp(a.rec_date)<=96*3600 and a.sign_date > '1999-01-01 00:00:00' and a.rec_date > '1999-01-01 00:00:00' then 1 else 0 end)   sign_num_4d,
        |  sum(case when to_unix_timestamp(a.sign_date)-to_unix_timestamp(a.rec_date)<=120*3600 and a.sign_date > '1999-01-01 00:00:00' and a.rec_date > '1999-01-01 00:00:00' then 1 else 0 end)  sign_num_5d,
        |  sum(case when to_unix_timestamp(a.sign_date)-to_unix_timestamp(a.rec_date)<=240*3600 and a.sign_date > '1999-01-01 00:00:00' and a.rec_date > '1999-01-01 00:00:00' then 1 else 0 end)  sign_num_10d,
        |  0 as in_time_rec_0_24,
        |  0 as no_in_time_rec_0_24,
        |  0 as until_now_no_rec_0_24,
        |  0 as lack_rec_0_24,
        |  0 as out_rec_0_24
        |from tmp.tmp_time_order_monitor_dig_source_table_0514 a
        |where a.order_create_date > '1999-01-01 00:00:00'
        |and a.rec_date > '1999-01-01 00:00:00'
        |group by
        |  substr(a.order_create_date,0,10),
        |  a.cust_name,
        |  a.cust_code,
        |  a.forecast_rec_site_id,
        |  a.channel_code,
        |  substr(a.rec_date,0,10)
        |union all
        |select
        |  substr(a.order_create_date,0,10) as order_date,
        |  a.rec_site_id as site_id,
        |  --按照名单发放
        |  a.cust_code,
        |  a.cust_name,
        |  a.channel_code as bill_type,
        |  substr(a.rec_date,0,10) as sum_day,
        |  2 as statis_type,
        |  a.rec_time_range as time_range,
        |  --新增
        |  0 as should_rec_cnt_18_18,
        |  0 as order_num_0_18,
        |  0 as order_num_18_0,
        |  0 as had_rec_cnt_18_18,
        |  0 as in_time_rec_cnt_18_18,
        |  0 as not_rec_cnt_18_18,
        |  0 as rec_num_0_18,
        |  0 as rec_num_18_0,
        |  0 as not_rec_num_0_18,
        |  0 as not_rec_num_18_0,
        |  0 as order_num,
        |  --新增
        |  0 as send_rec_cnt,
        |  0 as in_transit_center_cnt,
        |  0 as in_time_hand_cnt,
        |  0 as not_send_cnt,
        |  0 as in_center_not_come_cnt,
        |  0 as district_time_after_cnt,
        |  0 as not_send_0_18,
        |  0 as not_send_18_0,
        |  0 as in_time_rec_0_18,
        |  0 as in_time_rec_18_0,
        |  0 as no_in_time_rec_0_18,
        |  0 as no_in_time_rec_18_0,
        |  --这里只做揽收判断 明细补偿措施无揽收操作用forecast_first_site_id代替 20200325修改
        |  sum(case when (a.rec_site_id >0) then 1 else 0 end) as rec_num,
        |  0 as range_order_num,
        |  0 as over_rec_num,
        | sum(case
        |	   when a.rec_time_range = 6  and ( a.rec_site_id > 0 or a.disp_site_id > 0 or a.dpm_site_id > 0 or a.sign_site_id > 0 ) then 1
        |	   when a.rec_time_range = 12 and ( a.rec_site_id > 0 or a.disp_site_id > 0 or a.dpm_site_id > 0 or a.sign_site_id > 0 ) then 1
        |	   when a.rec_time_range = 24 and ( a.rec_site_id > 0 or a.disp_site_id > 0 or a.dpm_site_id > 0 or a.sign_site_id > 0 ) then 1
        |	   when a.rec_time_range = 48 and ( a.rec_site_id > 0 or a.disp_site_id > 0 or a.dpm_site_id > 0 or a.sign_site_id > 0 ) then 1
        |	   when a.rec_time_range = 72 and ( a.rec_site_id > 0 or a.disp_site_id > 0 or a.dpm_site_id > 0 or a.sign_site_id > 0 ) then 1
        |	   when a.rec_time_range = 1  and ( a.rec_site_id > 0 or a.disp_site_id > 0 or a.dpm_site_id > 0 or a.sign_site_id > 0 ) then 1 else 0 end) as range_rec_num,
        |  sum(case
        |  	 when  a.rec_time_range = 6  and a.rec_site_id > 0  and a.fir_send_center_site_id = 0 and a.first_center_id = 0 and a.disp_site_id = 0 and a.sign_site_id = 0 and a.dpm_site_id = 0  then 1
        |     when  a.rec_time_range = 12 and a.rec_site_id > 0  and a.fir_send_center_site_id = 0 and a.first_center_id = 0 and a.disp_site_id = 0 and a.sign_site_id = 0 and a.dpm_site_id = 0  then 1
        |	   when  a.rec_time_range = 24 and a.rec_site_id > 0  and a.fir_send_center_site_id = 0 and a.first_center_id = 0 and a.disp_site_id = 0 and a.sign_site_id = 0 and a.dpm_site_id = 0  then 1
        |	   when  a.rec_time_range = 48 and a.rec_site_id > 0  and a.fir_send_center_site_id = 0 and a.first_center_id = 0 and a.disp_site_id = 0 and a.sign_site_id = 0 and a.dpm_site_id = 0  then 1
        |	   when  a.rec_time_range = 72 and a.rec_site_id > 0  and a.fir_send_center_site_id = 0 and a.first_center_id = 0 and a.disp_site_id = 0 and a.sign_site_id = 0 and a.dpm_site_id = 0  then 1
        |	   when  a.rec_time_range = 1  and a.rec_site_id > 0  and a.fir_send_center_site_id = 0 and a.first_center_id = 0 and a.disp_site_id = 0 and a.sign_site_id = 0 and a.dpm_site_id = 0  then 1 else 0 end) as over_send_num,
        |   sum(case
        |	   when  a.rec_time_range = 6  and a.rec_site_id > 0  and ( a.fir_send_center_site_id > 0  or a.first_center_id > 0 or a.disp_site_id > 0 or a.sign_site_id > 0 or a.dpm_site_id > 0 ) then 1
        |     when  a.rec_time_range = 12 and a.rec_site_id > 0  and ( a.fir_send_center_site_id > 0  or a.first_center_id > 0 or a.disp_site_id > 0 or a.sign_site_id > 0 or a.dpm_site_id > 0 ) then 1
        |	   when  a.rec_time_range = 24 and a.rec_site_id > 0  and ( a.fir_send_center_site_id > 0  or a.first_center_id > 0 or a.disp_site_id > 0 or a.sign_site_id > 0 or a.dpm_site_id > 0 ) then 1
        |	   when  a.rec_time_range = 48 and a.rec_site_id > 0  and ( a.fir_send_center_site_id > 0  or a.first_center_id > 0 or a.disp_site_id > 0 or a.sign_site_id > 0 or a.dpm_site_id > 0 ) then 1
        |	   when  a.rec_time_range = 72 and a.rec_site_id > 0  and ( a.fir_send_center_site_id > 0  or a.first_center_id > 0 or a.disp_site_id > 0 or a.sign_site_id > 0 or a.dpm_site_id > 0 ) then 1
        |	   when  a.rec_time_range = 1  and a.rec_site_id > 0  and ( a.fir_send_center_site_id > 0  or a.first_center_id > 0 or a.disp_site_id > 0 or a.sign_site_id > 0 or a.dpm_site_id > 0 ) then 1 else 0 end) as range_send_num,
        |  sum(case when (a.first_center_id>0 or a.last_center_id>0 or a.disp_site_id >0 or a.sign_site_id >0) then 1 else 0 end) as send_num,
        |  sum(case when (a.first_center_id = 0 and a.sign_site_id = 0 and a.last_center_id = 0 and a.disp_site_id = 0) then 1 else 0 end) as not_send_num,
        |  --市场部的截止揽收下的签收情况
        |  sum(case when (a.sign_site_id >0 or a.dpm_site_id >0 ) then 1 else 0 end) as sign_num,
        |  0 as op_sign_num,
        |  0 as range_sign_num,
        |  0 as over_sign_num,
        |  0 as op_disp_num,
        |  0 as range_disp_num,
        |  0 as over_disp_num,
        |  sum(case when to_unix_timestamp(a.first_center_come_date)-to_unix_timestamp(a.rec_date)<=43200  and  a.first_center_come_date >'1999-01-01 00:00:00' and a.rec_date > '1999-01-01 00:00:00' then 1 else 0 end)  send_num_12,
        |  sum(case when to_unix_timestamp(a.first_center_come_date)-to_unix_timestamp(a.rec_date)<=86400  and  a.first_center_come_date >'1999-01-01 00:00:00' and a.rec_date > '1999-01-01 00:00:00' then 1 else 0 end)  send_num_24,
        |  sum(case when to_unix_timestamp(a.first_center_come_date)-to_unix_timestamp(a.rec_date)<=172800 and  a.first_center_come_date >'1999-01-01 00:00:00' and a.rec_date > '1999-01-01 00:00:00' then 1 else 0 end)  send_num_48,
        |  sum(case when to_unix_timestamp(a.first_center_come_date)-to_unix_timestamp(a.rec_date)>172800  and  a.first_center_come_date >'1999-01-01 00:00:00' and a.rec_date > '1999-01-01 00:00:00' then 1 else 0 end)  send_num_48_hup,
        |  sum(case when to_unix_timestamp(a.sign_date)-to_unix_timestamp(a.rec_date)<=86400 and a.sign_date > '1999-01-01 00:00:00' and a.rec_date > '1999-01-01 00:00:00' then 1 else 0 end)     sign_num_24,
        |  sum(case when to_unix_timestamp(a.sign_date)-to_unix_timestamp(a.rec_date)<=172800 and a.sign_date > '1999-01-01 00:00:00' and a.rec_date > '1999-01-01 00:00:00' then 1 else 0 end)    sign_num_48,
        |  sum(case when to_unix_timestamp(a.sign_date)-to_unix_timestamp(a.rec_date)<=72*3600 and a.sign_date > '1999-01-01 00:00:00' and a.rec_date > '1999-01-01 00:00:00' then 1 else 0 end)   sign_num_3d,
        |  sum(case when to_unix_timestamp(a.sign_date)-to_unix_timestamp(a.rec_date)<=96*3600 and a.sign_date > '1999-01-01 00:00:00' and a.rec_date > '1999-01-01 00:00:00' then 1 else 0 end)   sign_num_4d,
        |  sum(case when to_unix_timestamp(a.sign_date)-to_unix_timestamp(a.rec_date)<=120*3600 and a.sign_date > '1999-01-01 00:00:00' and a.rec_date > '1999-01-01 00:00:00' then 1 else 0 end)  sign_num_5d,
        |  sum(case when to_unix_timestamp(a.sign_date)-to_unix_timestamp(a.rec_date)<=240*3600 and a.sign_date > '1999-01-01 00:00:00' and a.rec_date > '1999-01-01 00:00:00' then 1 else 0 end)  sign_num_10d,
        |  0 as in_time_rec_0_24,
        |  0 as no_in_time_rec_0_24,
        |  0 as until_now_no_rec_0_24,
        |  0 as lack_rec_0_24,
        |  0 as out_rec_0_24
        |from tmp.tmp_time_order_monitor_dig_source_table_0514 a
        |--保证有实际揽收操作
        |where a.rec_site_id > 0
        |and a.order_create_date > '1999-01-01 00:00:00'
        |and a.rec_date > '1999-01-01 00:00:00'
        |group by
        |  substr(a.order_create_date,0,10),
        |  a.cust_name,
        |  a.cust_code,
        |  a.rec_site_id,
        |  a.channel_code,
        |  substr(a.rec_date,0,10),
        |  a.rec_time_range
        |union all
        |select
        |  substr(a.order_create_date,0,10) as order_date,
        |  a.disp_site_id as site_id,
        |  --按照名单发放
        |  a.cust_code,
        |  a.cust_name,
        |  a.channel_code as bill_type,
        |  substr(a.disp_date,0,10) as sum_day,
        |  0 as statis_type,
        |  a.disp_time_range as time_range,
        |  --新增
        |  0 as should_rec_cnt_18_18,
        |  0 as order_num_0_18,
        |  0 as order_num_18_0,
        |  0 as had_rec_cnt_18_18,
        |  0 as in_time_rec_cnt_18_18,
        |  0 as not_rec_cnt_18_18,
        |  0 as rec_num_0_18,
        |  0 as rec_num_18_0,
        |  0 as not_rec_num_0_18,
        |  0 as not_rec_num_18_0,
        |  0 as order_num,
        |  --新增
        |  0 as send_rec_cnt,
        |  0 as in_transit_center_cnt,
        |  0 as in_time_hand_cnt,
        |  0 as not_send_cnt,
        |  0 as in_center_not_come_cnt,
        |  0 as district_time_after_cnt,
        |  0 as not_send_0_18,
        |  0 as not_send_18_0,
        |  0 as in_time_rec_0_18,
        |  0 as in_time_rec_18_0,
        |  0 as no_in_time_rec_0_18,
        |  0 as no_in_time_rec_18_0,
        |  0 as rec_num,
        |  0 as range_order_num,
        |  0 as over_rec_num,
        |  0 as range_rec_num,
        |  0 as over_send_num,
        |  0 as range_send_num,
        |  0 as send_num,
        |  0 as not_send_num,
        |  0 as sign_num,
        |  sum(case when a.disp_site_id > 0 and (a.sign_site_id > 0 or a.dpm_site_id >0 ) then 1 else 0 end) as op_sign_num,
        |  sum(case
        |	   when a.disp_time_range = 6   and (a.sign_site_id > 0 or a.dpm_site_id >0 ) then 1
        |	   when a.disp_time_range = 12  and (a.sign_site_id > 0 or a.dpm_site_id >0 ) then 1
        |	   when a.disp_time_range = 24  and (a.sign_site_id > 0 or a.dpm_site_id >0 ) then 1
        |	   when a.disp_time_range = 48  and (a.sign_site_id > 0 or a.dpm_site_id >0 ) then 1
        |	   when a.disp_time_range = 72  and (a.sign_site_id > 0 or a.dpm_site_id >0 ) then 1
        |	   when a.disp_time_range = 1   and (a.sign_site_id > 0 or a.dpm_site_id >0 ) then 1 else 0 end ) as range_sign_num,
        |  sum(case
        |	   when  a.disp_time_range = 6  and a.disp_site_id > 0 and a.sign_site_id = 0 and a.dpm_site_id = 0  then 1
        |     when  a.disp_time_range = 12 and a.disp_site_id > 0 and a.sign_site_id = 0 and a.dpm_site_id = 0  then 1
        |	   when  a.disp_time_range = 24 and a.disp_site_id > 0 and a.sign_site_id = 0 and a.dpm_site_id = 0  then 1
        |	   when  a.disp_time_range = 48 and a.disp_site_id > 0 and a.sign_site_id = 0 and a.dpm_site_id = 0  then 1
        |	   when  a.disp_time_range = 72 and a.disp_site_id > 0 and a.sign_site_id = 0 and a.dpm_site_id = 0  then 1
        |	   when  a.disp_time_range = 1  and a.disp_site_id > 0 and a.sign_site_id = 0 and a.dpm_site_id = 0  then 1 else 0 end) as over_sign_num,
        |  sum(case when a.disp_site_id > 0 then 1 else 0 end) as op_disp_num,
        |  sum(case
        |	  when a.disp_time_range = 6   and ( a.disp_site_id > 0 or a.sign_site_id > 0 or a.dpm_site_id > 0 ) then 1
        |	  when a.disp_time_range = 12  and ( a.disp_site_id > 0 or a.sign_site_id > 0 or a.dpm_site_id > 0 ) then 1
        |	  when a.disp_time_range = 24  and ( a.disp_site_id > 0 or a.sign_site_id > 0 or a.dpm_site_id > 0 ) then 1
        |	  when a.disp_time_range = 48  and ( a.disp_site_id > 0 or a.sign_site_id > 0 or a.dpm_site_id > 0 ) then 1
        |	  when a.disp_time_range = 72  and ( a.disp_site_id > 0 or a.sign_site_id > 0 or a.dpm_site_id > 0 ) then 1
        |	  when a.disp_time_range = 1   and ( a.disp_site_id > 0 or a.sign_site_id > 0 or a.dpm_site_id > 0 ) then 1  else 0 end) as range_disp_num,
        |  0 as over_disp_num,
        |  0 as send_num_12,
        |  0 as send_num_24,
        |  0 as send_num_48,
        |  0 as send_num_48_hup,
        |  0 as sign_num_24,
        |  0 as sign_num_48,
        |  0 as sign_num_3d,
        |  0 as sign_num_4d,
        |  0 as sign_num_5d,
        |  0 as sign_num_10d,
        |  0 as in_time_rec_0_24,
        |  0 as no_in_time_rec_0_24,
        |  0 as until_now_no_rec_0_24,
        |  0 as lack_rec_0_24,
        |  0 as out_rec_0_24
        |from tmp.tmp_time_order_monitor_dig_source_table_0514 a
        |where a.order_create_date > '1999-01-01 00:00:00'
        |and a.disp_date > '1999-01-01 00:00:00'
        |group by
        |  substr(a.order_create_date,0,10),
        |  a.cust_name,
        |  a.cust_code,
        |  a.disp_site_id,
        |  a.channel_code,
        |  substr(a.disp_date,0,10),
        |  a.disp_time_range
        |union all
        |select
        |  substr(a.order_create_date,0,10) as order_date,
        |  case when a.rec_site_id > 0 then a.rec_site_id else a.forecast_rec_site_id end as site_id,
        |  --按照名单发放
        |  a.cust_code,
        |  a.cust_name,
        |  a.channel_code as bill_type,
        |  substr(a.order_create_date,0,10) as sum_day,
        |  0 as statis_type,
        |  a.order_time_range as time_range,
        |  --新增
        |  0 as should_rec_cnt_18_18,
        |  0 as order_num_0_18,
        |  0 as order_num_18_0,
        |  0 as had_rec_cnt_18_18,
        |  0 as in_time_rec_cnt_18_18,
        |  0 as not_rec_cnt_18_18,
        |  0 as rec_num_0_18,
        |  0 as rec_num_18_0,
        |  0 as not_rec_num_0_18,
        |  0 as not_rec_num_18_0,
        |  count(1) as order_num,
        |  --新增
        |  0 as send_rec_cnt,
        |  0 as in_transit_center_cnt,
        |  0 as in_time_hand_cnt,
        |  0 as not_send_cnt,
        |  0 as in_center_not_come_cnt,
        |  0 as district_time_after_cnt,
        |  0 as not_send_0_18,
        |  0 as not_send_18_0,
        |  0 as in_time_rec_0_18,
        |  0 as in_time_rec_18_0,
        |  0 as no_in_time_rec_0_18,
        |  0 as no_in_time_rec_18_0,
        |  0 as rec_num,
        |  sum(case
        |	   when a.order_time_range = 6  and a.forecast_rec_site_id >0  then 1
        |	   when a.order_time_range = 12 and a.forecast_rec_site_id >0  then 1
        |	   when a.order_time_range = 24 and a.forecast_rec_site_id >0  then 1
        |	   when a.order_time_range = 48 and a.forecast_rec_site_id >0  then 1
        |	   when a.order_time_range = 72 and a.forecast_rec_site_id >0  then 1
        |	   when a.order_time_range = 1  and a.forecast_rec_site_id >0  then 1 else 0 end) as range_order_num,
        |  sum(case
        |	   when a.order_time_range = 6  and a.forecast_rec_site_id >0  and a.rec_site_id = 0  and a.disp_site_id = 0 and a.sign_site_id = 0 and a.dpm_site_id = 0  then 1
        |     when a.order_time_range = 12 and a.forecast_rec_site_id >0  and a.rec_site_id = 0  and a.disp_site_id = 0 and a.sign_site_id = 0 and a.dpm_site_id = 0  then 1
        |	   when a.order_time_range = 24 and a.forecast_rec_site_id >0  and a.rec_site_id = 0  and a.disp_site_id = 0 and a.sign_site_id = 0 and a.dpm_site_id = 0  then 1
        |	   when a.order_time_range = 48 and a.forecast_rec_site_id >0  and a.rec_site_id = 0  and a.disp_site_id = 0 and a.sign_site_id = 0 and a.dpm_site_id = 0  then 1
        |	   when a.order_time_range = 72 and a.forecast_rec_site_id >0  and a.rec_site_id = 0  and a.disp_site_id = 0 and a.sign_site_id = 0 and a.dpm_site_id = 0  then 1
        |	   when a.order_time_range = 1  and a.forecast_rec_site_id >0  and a.rec_site_id = 0  and a.disp_site_id = 0 and a.sign_site_id = 0 and a.dpm_site_id = 0  then 1 else 0 end) as over_rec_num,
        |    0 as range_rec_num,
        |    0 as over_send_num,
        |    0 as range_send_num,
        |  0 as send_num,
        |  0 as not_send_num,
        |  0 as sign_num,
        |  0 as op_sign_num,
        |  0 as range_sign_num,
        |  0 as over_sign_num,
        |  0 as op_disp_num,
        |  0 as range_disp_num,
        |  0 as over_disp_num,
        |  0 as send_num_12,
        |  0 as send_num_24,
        |  0 as send_num_48,
        |  0 as send_num_48_hup,
        |  0 as sign_num_24,
        |  0 as sign_num_48,
        |  0 as sign_num_3d,
        |  0 as sign_num_4d,
        |  0 as sign_num_5d,
        |  0 as sign_num_10d,
        |  -- 及时揽收量 0点-24点
        |  sum(case when (rec_site_id > 0 and rec_date <= deadline_rec_date) then 1 else 0 end) as in_time_rec_0_24,
        |  -- 未及时揽收量 0点-24点
        |  sum(
        |	   CASE WHEN
        |	   (
        |	   		(
        |	   			rec_site_id > 0
        |	   			AND rec_date > deadline_rec_date
        |	   		)
        |	   		OR (
        |	   			a.rec_site_id = 0
        |	   			AND a.disp_site_id = 0
        |	   			AND a.dpm_site_id = 0
        |	   			AND a.first_center_id = 0
        |	   			AND a.last_center_id = 0
        |	   			AND from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') > a.deadline_rec_date
        |	   		)
        |	   ) THEN
        |	   	1
        |	   ELSE
        |	   	0
        |	   END
        |    ) as no_in_time_rec_0_24,
        |   -- 截至当前未揽收量 0点-24点
        |  sum(case when rec_site_id = 0
        |            and disp_site_id = 0
        |			and dpm_site_id = 0
        |			and first_center_id = 0
        |			and last_center_id = 0
        |			and sign_site_id = 0
        |		then 1 else 0 end) as until_now_no_rec_0_24,
        |   -- 揽收记录缺失 0点-24点
        |  sum(
        |     case when
        |         rec_site_id = 0 and
        |         (disp_site_id <> 0 or dpm_site_id <> 0 or first_center_id <> 0 or last_center_id <> 0 or sign_site_id <> 0)
        |		 then 1 else 0 end
        |	 ) as lack_rec_0_24,
        |   -- 超时揽收 0点-24点
        |  sum(
        |	   CASE WHEN
        |	   (
        |	   	rec_site_id > 0
        |	   	AND rec_date > deadline_rec_date
        |	   ) THEN 1 ELSE 0 END
        |     ) as out_rec_0_24
        |from tmp.tmp_time_order_monitor_dig_source_table_0514 a
        |where a.order_create_date > '1999-01-01 00:00:00'
        |group by
        |  substr(a.order_create_date,0,10),
        |  a.cust_name,
        |  a.cust_code,
        |  case when a.rec_site_id > 0 then a.rec_site_id else a.forecast_rec_site_id end,
        |  a.channel_code,
        |  a.order_time_range
        |union all
        |select
        |  substr(a.order_create_date,0,10) as order_date,
        |  a.forecast_disp_site_id as site_id,
        |  a.cust_code,
        |  a.cust_name,
        |  a.channel_code as bill_type,
        |  case when a.last_center_come_date = '1999-01-01 00:00:00' then substr(a.last_center_send_date,0,10) else substr(a.last_center_come_date,0,10) end as sum_day,
        |  0 as statis_type,
        |  a.center_time_range as time_range,
        |  0 as should_rec_cnt_18_18,
        |  0 as order_num_0_18,
        |  0 as order_num_18_0,
        |  0 as had_rec_cnt_18_18,
        |  0 as in_time_rec_cnt_18_18,
        |  0 as not_rec_cnt_18_18,
        |  0 as rec_num_0_18,
        |  0 as rec_num_18_0,
        |  0 as not_rec_num_0_18,
        |  0 as not_rec_num_18_0,
        |  0 as order_num,
        |  --新增
        |  0 as send_rec_cnt,
        |  0 as in_transit_center_cnt,
        |  0 as in_time_hand_cnt,
        |  0 as not_send_cnt,
        |  0 as in_center_not_come_cnt,
        |  0 as district_time_after_cnt,
        |  0 as not_send_0_18,
        |  0 as not_send_18_0,
        |  0 as in_time_rec_0_18,
        |  0 as in_time_rec_18_0,
        |  0 as no_in_time_rec_0_18,
        |  0 as no_in_time_rec_18_0,
        |  0 as rec_num,
        |  0 as range_order_num,
        |  0 as over_rec_num,
        |  0 as range_rec_num,
        |  0 as over_send_num,
        |  0 as range_send_num,
        |  0 as send_num,
        |  0 as not_send_num,
        |  0 as sign_num,
        |  0 as op_sign_num,
        |  0 as range_sign_num,
        |  0 as over_sign_num,
        |  0 as op_disp_num,
        |  0 as range_disp_num,
        |  sum(case
        |	when  a.center_time_range = 6  and a.disp_site_id = 0  and a.sign_site_id = 0 and a.dpm_site_id = 0    and a.forecast_disp_site_id > 0  then 1
        |  when  a.center_time_range = 12 and a.disp_site_id = 0    and a.sign_site_id = 0 and a.dpm_site_id = 0  and a.forecast_disp_site_id > 0  then 1
        |	when  a.center_time_range = 24 and a.disp_site_id = 0  and a.sign_site_id = 0 and a.dpm_site_id = 0    and a.forecast_disp_site_id > 0  then 1
        |	when  a.center_time_range = 48 and a.disp_site_id = 0  and a.sign_site_id = 0 and a.dpm_site_id = 0    and a.forecast_disp_site_id > 0  then 1
        |	when  a.center_time_range = 72 and a.disp_site_id = 0  and a.sign_site_id = 0 and a.dpm_site_id = 0    and a.forecast_disp_site_id > 0  then 1
        |	when  a.center_time_range = 1  and a.disp_site_id = 0  and a.sign_site_id = 0 and a.dpm_site_id = 0    and a.forecast_disp_site_id > 0  then 1 else 0 end) as over_disp_num,
        |  0 as send_num_12,
        |  0 as send_num_24,
        |  0 as send_num_48,
        |  0 as send_num_48_hup,
        |  0 as sign_num_24,
        |  0 as sign_num_48,
        |  0 as sign_num_3d,
        |  0 as sign_num_4d,
        |  0 as sign_num_5d,
        |  0 as sign_num_10d,
        |  0 as in_time_rec_0_24,
        |  0 as no_in_time_rec_0_24,
        |  0 as until_now_no_rec_0_24,
        |  0 as lack_rec_0_24,
        |  0 as out_rec_0_24
        |from tmp.tmp_time_order_monitor_dig_source_table_0514 a
        |where a.order_create_date > '1999-01-01 00:00:00'
        |and (a.last_center_come_date > '1999-01-01 00:00:00' or a.last_center_send_date > '1999-01-01 00:00:00')
        |group by
        |  substr(a.order_create_date,0,10),
        |  a.cust_name,
        |  a.cust_code,
        |  a.forecast_disp_site_id,
        |  a.channel_code,
        |  case when a.last_center_come_date = '1999-01-01 00:00:00' then substr(a.last_center_send_date,0,10) else substr(a.last_center_come_date,0,10) end,
        |  a.center_time_range
        | ) a
        |GROUP BY
        |	a.order_date,
        |	a.site_id,
        |	a.cust_code,
        |	a.cust_name,
        |	a.bill_type,
        |	a.sum_day,
        |	a.statis_type,
        |  a.time_range
        |""".stripMargin).show(10, false)
    println("sql执行耗时：" + (System.currentTimeMillis() - start))
  }

  def main(args: Array[String]): Unit = {
    /*this.init(10, false)
    this.stop*/
    FireUtils.retry(10, 10) {
      val i = 10 / 0
    }
  }
}
