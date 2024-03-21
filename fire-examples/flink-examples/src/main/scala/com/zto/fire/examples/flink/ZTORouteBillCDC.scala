package com.zto.fire.examples.flink

import com.zto.fire.common.anno.Config
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming
import com.zto.fire._

/**
 * 运单宽表binlog cdc 数据
 */

@Config(
  """
    |flink.stream.number.execution.retries=3
    |flink.stream.time.characteristic=ProcessingTime
    |flink.force.kryo.enable=false
    |flink.force.avro.enable=false
    |flink.generic.types.enable=true
    |flink.execution.mode=pipelined
    |flink.auto.type.registration.enable=true
    |flink.auto.generate.uid.enable=true
    |flink.max.parallelism=1536
    |""")
@Streaming(interval = 300, timeout = 1800, unaligned = false, pauseBetween = 180,
  concurrent = 1, failureNumber = 3, parallelism = 3, autoStart = true, disableOperatorChaining = true)
object ZTORouteBillCDC extends FlinkStreaming {
  override def process(): Unit = {

//    val dbTable = parameter.get("dbTable", "lyznhb_ml.zto_route_bill_cdc_test")
//    println(s"dbTable : $dbTable")

    val rocketMQTableName = "zto_route_bill_cdc"
    val kafkaSql =
      s"""
         |create table $rocketMQTableName (
         | `row_kind` STRING METADATA FROM 'value.row_kind' VIRTUAL,
         | `mq_topic` STRING METADATA FROM 'topic' VIRTUAL,
         | `mq_broker` STRING METADATA FROM 'broker' VIRTUAL,
         | `mq_queue_id` INT METADATA FROM 'queue_id' VIRTUAL,
         | `mq_offset` BIGINT METADATA FROM 'offset' VIRTUAL,
         | `mq_timestamp` TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL,
         | `id` bigint,
         | `customize_id` STRING,
         | `bill_code` STRING,
         | `order_code` STRING,
         | `fst_code` STRING,
         | `operate_weight` decimal(10, 2),
         | `estimate_weight` decimal(10, 2),
         | `estimate_volume` decimal(10, 2),
         | `goods_type` STRING,
         | `rec_site_id` BIGINT,
         | `disp_site_id` BIGINT,
         | `rec_scan_time` STRING,
         | `disp_scan_time` STRING,
         | `sign_scan_time` STRING,
         | `plan_sign_time` STRING,
         | `route_code` STRING,
         | `route_index` INT,
         | `route_type` STRING,
         | `ori_site_id` BIGINT,
         | `des_site_id` BIGINT,
         | `ori_unload_code` STRING,
         | `des_unload_code` STRING,
         | `send_site_id` BIGINT,
         | `come_site_id` BIGINT,
         | `send_scan_time` STRING,
         | `come_scan_time` STRING,
         | `last_send_time` STRING,
         | `last_come_time` STRING,
         | `crossing_type` INT,
         | `line_name` STRING,
         | `line_number` STRING,
         | `time_cost` INT,
         | `transport_type` STRING,
         | `unload_port` STRING,
         | `owner_bag_no` STRING,
         | `exec_status` INT,
         | `truck_number` STRING,
         | `truck_tail_number` STRING,
         | `truck_sign_code` STRING,
         | `batch_number` STRING,
         | `schedule_id` STRING,
         | `plan_send_time` STRING,
         | `plan_arrive_time` STRING,
         | `actual_send_time` STRING,
         | `actual_arrive_time` STRING,
         | `start_hours` STRING,
         | `operate_time` INT,
         | `clear_store_time` STRING,
         | `receive_time` STRING,
         | `dispatch_time` STRING,
         | `exception_type` INT,
         | `event_id` STRING,
         | `event_type` INT,
         | `is_blocked` INT,
         | `is_deleted` INT,
         | `remark` STRING,
         | `created_by` STRING,
         | `created_on` STRING,
         | `modified_by` STRING,
         | `modified_on` STRING,
         | `subarea_time` STRING,
         | `disp_way_in_time` STRING,
         | `route_situation` STRING,
         | `is_problem` INT,
         | `first_disp_time` STRING,
         | `first_disp_way_in_time` STRING,
         | `promise_time` STRING,
         | `intercept_status` INT,
         | `value_added_type` INT,
         | `last_rec_scan_time` STRING,
         | `clerk_rec_scan_time` STRING,
         | `packet_exception_type` BIGINT,
         | `packet_exception_effect` BIGINT,
         | `packet_exception_time` STRING,
         | `assessment_type` INT,
         | `assessment_site_id` BIGINT,
         | `predicate_assessment_time` STRING
         |) WITH (
         | 'connector' = 'rocketmq',
         | 'topic' = 'ROUTE_BILL_BINLOG',
         | 'properties.bootstrap.servers' = 'ip:9876',
         | 'properties.group.id' = 'ROUTE_BILL_BINLOG_FLINK_CONSUMER_VERIFY',
         | 'scan.startup.mode' = 'earliest-offset',
         | 'zdtp-json.cdc-write-hive' = 'false',
         | 'format' = 'zdtp-json'
         | )
      """.stripMargin

    this.fire.sql(kafkaSql)

    val printSql =
      """
        |create table print_table(
        | `row_kind` STRING,
        | `mq_topic` STRING,
        | `mq_broker` STRING ,
        | `mq_queue_id` INT ,
        | `mq_offset` BIGINT ,
        | `mq_timestamp` STRING ,
        | `id` bigint,
        | `customize_id` STRING,
        | `bill_code` STRING,
        | `order_code` STRING,
        | `fst_code` STRING,
        | `operate_weight` decimal(10, 2),
        | `estimate_weight` decimal(10, 2),
        | `estimate_volume` decimal(10, 2),
        | `goods_type` STRING,
        | `rec_site_id` BIGINT,
        | `disp_site_id` BIGINT,
        | `rec_scan_time` STRING,
        | `disp_scan_time` STRING,
        | `sign_scan_time` STRING,
        | `plan_sign_time` STRING,
        | `route_code` STRING,
        | `route_index` INT,
        | `route_type` STRING,
        | `ori_site_id` BIGINT,
        | `des_site_id` BIGINT,
        | `ori_unload_code` STRING,
        | `des_unload_code` STRING,
        | `send_site_id` BIGINT,
        | `come_site_id` BIGINT,
        | `send_scan_time` STRING,
        | `come_scan_time` STRING,
        | `last_send_time` STRING,
        | `last_come_time` STRING,
        | `crossing_type` INT,
        | `line_name` STRING,
        | `line_number` STRING,
        | `time_cost` INT,
        | `transport_type` STRING,
        | `unload_port` STRING,
        | `owner_bag_no` STRING,
        | `exec_status` INT,
        | `truck_number` STRING,
        | `truck_tail_number` STRING,
        | `truck_sign_code` STRING,
        | `batch_number` STRING,
        | `schedule_id` STRING,
        | `plan_send_time` STRING,
        | `plan_arrive_time` STRING,
        | `actual_send_time` STRING,
        | `actual_arrive_time` STRING,
        | `start_hours` STRING,
        | `operate_time` INT,
        | `clear_store_time` STRING,
        | `receive_time` STRING,
        | `dispatch_time` STRING,
        | `exception_type` INT,
        | `event_id` STRING,
        | `event_type` INT,
        | `is_blocked` INT,
        | `is_deleted` INT,
        | `remark` STRING,
        | `created_by` STRING,
        | `created_on` STRING,
        | `modified_by` STRING,
        | `modified_on` STRING,
        | `subarea_time` STRING,
        | `disp_way_in_time` STRING,
        | `route_situation` STRING,
        | `is_problem` INT,
        | `first_disp_time` STRING,
        | `first_disp_way_in_time` STRING,
        | `promise_time` STRING,
        | `intercept_status` INT,
        | `value_added_type` INT,
        | `last_rec_scan_time` STRING,
        | `clerk_rec_scan_time` STRING,
        | `packet_exception_type` BIGINT,
        | `packet_exception_effect` BIGINT,
        | `packet_exception_time` STRING,
        | `assessment_type` INT,
        | `assessment_site_id` BIGINT,
        | `predicate_assessment_time` STRING
        |) WITH (
        | 'connector' = 'print'
        |)
        |""".stripMargin

    this.fire.sql(printSql)
    //    checkToHive()

    val insertSql =
      s"""
         |insert into
         |print_table
         |select
         | row_kind,
         | mq_topic,
         | mq_broker,
         | mq_queue_id,
         | mq_offset,
         | DATE_FORMAT(mq_timestamp, 'yyyy-MM-dd HH:mm:ss.SSS') mq_timestamp,
         | id,
         | customize_id,
         | bill_code,
         | order_code,
         | fst_code,
         | operate_weight,
         | estimate_weight,
         | estimate_volume,
         | goods_type,
         | rec_site_id,
         | disp_site_id,
         | rec_scan_time,
         | disp_scan_time,
         | sign_scan_time,
         | plan_sign_time,
         | route_code,
         | route_index,
         | route_type,
         | ori_site_id,
         | des_site_id,
         | ori_unload_code,
         | des_unload_code,
         | send_site_id,
         | come_site_id,
         | send_scan_time,
         | come_scan_time,
         | last_send_time,
         | last_come_time,
         | crossing_type,
         | line_name,
         | line_number,
         | time_cost,
         | transport_type,
         | unload_port,
         | owner_bag_no,
         | exec_status,
         | truck_number,
         | truck_tail_number,
         | truck_sign_code,
         | batch_number,
         | schedule_id,
         | plan_send_time,
         | plan_arrive_time,
         | actual_send_time,
         | actual_arrive_time,
         | start_hours,
         | operate_time,
         | clear_store_time,
         | receive_time,
         | dispatch_time,
         | exception_type,
         | event_id,
         | event_type,
         | is_blocked,
         | is_deleted,
         | remark,
         | created_by,
         | created_on,
         | modified_by,
         | modified_on,
         | subarea_time,
         | disp_way_in_time,
         | route_situation,
         | is_problem,
         | first_disp_time,
         | first_disp_way_in_time,
         | promise_time,
         | intercept_status,
         | value_added_type,
         | last_rec_scan_time,
         | clerk_rec_scan_time,
         | packet_exception_type,
         | packet_exception_effect,
         | packet_exception_time,
         | assessment_type,
         | assessment_site_id,
         | predicate_assessment_time
         |from
         |default_catalog.default_database.$rocketMQTableName
      """.stripMargin
    this.fire.sql(insertSql)

  }
}
