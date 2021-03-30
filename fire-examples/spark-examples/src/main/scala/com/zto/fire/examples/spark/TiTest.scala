package com.zto.fire.examples.spark

import java.sql.ResultSet
import com.zto.fire._
import com.zto.fire.core.conf.EngineConf
import com.zto.fire.examples.bean.Student
import com.zto.fire.jdbc.conf.FireJdbcConf
import com.zto.fire.jdbc.JdbcConnector
import com.zto.fire.spark.BaseSparkCore
import org.apache.spark.sql.Row

import scala.collection.immutable

/**
 *
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2021-02-22 13:17
 */
object TiTest extends BaseSparkCore {

  def process1: Unit = {
    val tiDF = this.spark.sql(
      """
        |select
        |	id,
        |	bill_code,
        |	order_code,
        |	order_create_date,
        |	earliest_order_create_date,
        |	latest_order_create_date,
        |	assign_site_code,
        |	forecast_disp_site_code,
        |	forecast_rec_site_id,
        |	forecast_disp_site_id,
        |	three_code,
        |	send_name,
        |	receive_name,
        |	send_mobile,
        |	receive_mobile,
        |	send_address,
        |	receive_address,
        |	partner_id,
        |	send_prov_id,
        |	receive_prov_id,
        |	send_city_id,
        |	receive_city_id,
        |	send_district_id,
        |	receive_district_id,
        |	channel_code,
        |	forecast_first_center_id,
        |	forecast_last_center_id,
        |	deadline_rec_date,
        |	forecast_first_center_date,
        |	forecast_last_center_date,
        |	is_start_link,
        |	earliest_rec_site_id,
        |	earliest_rec_site_date,
        |	rec_site_id,
        |	rec_site_date,
        |	rec_weight,
        |	rec_send_date,
        |	max_rec_weight,
        |	rec_emp_code,
        |	rec_emp_name,
        |	next_site_id_after_rec,
        |	next_site_after_rec_come_date,
        |	next_site_after_rec_send_date,
        |	first_center_transfer_id,
        |	first_center_id,
        |	first_center_come_date,
        |	first_center_send_date,
        |	first_center_send_car_code,
        |	first_center_car_send_scan_date,
        |	last_center_transfer_id,
        |	last_center_id,
        |	last_center_come_date,
        |	last_center_send_date,
        |	max_center_weight,
        |	disp_site_id,
        |	disp_site_date,
        |	disp_emp_code,
        |	disp_emp_name,
        |	dpm_site_id,
        |	dpm_site_date,
        |	dpm_company_code,
        |	is_local_dpm,
        |	sign_site_id,
        |	sign_site_date,
        |	is_return,
        |	is_problem,
        |	is_interceptor,
        |	bill_state,
        |	latest_scan_type,
        |	latest_scan_site_id,
        |	latest_scan_site_date,
        |	pre_scan_site_id,
        |	pre_scan_site_date,
        |	pre_scan_site_type,
        |	is_airway,
        |	first_scan_site_id,
        |	first_scan_type,
        |	first_scan_date,
        |	is_guo_guo,
        |	elec_site_id,
        |	custom_code,
        |	custom_name,
        |	forecast_sign_date_v1,
        |	forecast_sign_date_v2,
        |	forecast_disp_emp_code,
        |	forecast_disp_emp_name,
        |	elec_create_date,
        |	is_disp_intercept,
        |	intercept_status,
        |	actual_intercept_date,
        |	actual_intercept_site_code,
        |	actual_intercept_site_id,
        |	intercept_emp_code,
        |	intercept_emp_name,
        |	intercept_site_code,
        |	intercept_site_id,
        |	forecast_rec_site_league_id,
        |	forecast_disp_site_league_id,
        |	earliest_rec_site_league_id,
        |	rec_site_league_id,
        |	disp_site_league_id,
        |	sign_site_league_id,
        |	last_center_car_code,
        |	forecast_disp_start_date,
        |	forecast_disp_end_date,
        |	forecast_first_center_end_date,
        |	forecast_first_center_car_date,
        |	forecast_first_center_latest_car_date,
        |	first_center_gps_start_date,
        |	forecast_last_center_pull_site_date,
        |	forecast_last_center_pull_site_rec_date,
        |	second_center_id,
        |	second_center_come_date,
        |	second_center_send_date,
        |	is_timeliness,
        |	next_site_id_after_center,
        |	xmf_order_create_date,
        |	xmf_channal_code,
        |	forecast_rec_last_center_come_date,
        |	xmf_site_route_last_date,
        |	is_university,
        |	forecast_disp_no,
        |	bill_router_line,
        |	forecast_last_center_pull_site_id,
        |	rec_kpi_transfer_center_id,
        |	rec_kpi_center_id
        |from tmp.tidb_to_clickhouse t
        |""".stripMargin).cache()
    println("总记录数：" + tiDF.count())
    /*this.spark.sql("use tmp")
    tiDF.write.mode(SaveMode.Overwrite).saveAsTable("tmp.tidb_to_clickhouse")*/
    tiDF.coalesce(99).jdbcBatchUpdate("insert into default.zto_ss_bill_order_detail_base values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", batch = 3000)
  }

  override def process: Unit = {
    val df = this.spark.createDataFrame(Student.newStudentList(), classOf[Student])
    df.repartition(10).foreachPartition((it: Iterator[Row]) => {
        JdbcConnector.executeQueryCall("select count(1) from default.zto_ss_bill_order_detail_base", callback = rs => {
          println("结果集：" + rs)
          1
        })
    })
    println("driver:" + FireJdbcConf.driverClass(1))
    Thread.currentThread().join()
  }

  def getA(a: Int): String = {
    println("执行getA")
    "执行getA"
  }

  def main(args: Array[String]): Unit = {
    /*this.init()
    this.stop*/
    var a = 1
    tryWithLog {
      println("执行block")
      a = a + 1
      println(a)
    } (this.logger, getA(a))
  }
}
