/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zto.fire.common.util

import org.junit.Test
import com.zto.fire.common.util.SQLUtils._

/**
 * SQLUtils单元测试
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-11-26 15:11
 */
class SQLUtilsTest {

  @Test
  def testParse: Unit = {
    val selectSql =
      """
        | select * FROM
        | student1 s join dev.teacher2 b
        |""".stripMargin
    tableParse(selectSql).foreach(tableName => println("匹配：" + tableName))

    val insertSQL =
      """
        |insert into dev.student3(id,name) values(1, 'root');
        |insert into teacher4(id,name) values(1, 'root');
        |""".stripMargin
    tableParse(insertSQL).foreach(tableName => println("匹配：" + tableName))

    val deleteSQL =
      """
        |delete from teacher5 where id=10;
        |delete from dev.teacher6 where id=10;
        |""".stripMargin
    tableParse(deleteSQL).foreach(tableName => println("匹配：" + tableName))

    val createSQL =
      """
        |create table hello7(idxxx);
        |create table if not EXISTS hello8;
        |CREATE TABLE student9 LIKE tmp.student10
        |""".stripMargin
    tableParse(createSQL).foreach(tableName => println("匹配：" + tableName))

    val alterSQL =
      """
        |LOAD DATA LOCAL INPATH '/home/hadoop/data/student1.txt' INTO TABLE student11
        |""".stripMargin
    tableParse(alterSQL).foreach(tableName => println("匹配：" + tableName))

    val testSQL =
      """
        |create table table_student12
        |insert into dev.student13_from
        |delete from `from_student14_from`
        |select * from (select * from student15)
        |select * from (select * from
        |student16)
        |""".stripMargin
    tableParse(testSQL).foreach(tableName => println("匹配：" + tableName))

    val start = System.currentTimeMillis()
    (1 to 1000).foreach(i => tableParse(selectSql))
    println("耗时：" + (System.currentTimeMillis() - start))
  }

  @Test
  def testParsePlaceholder: Unit = {
    val insert = "insert into user(name, age, id) values(?, ?, 10) ON DUPLICATE KEY UPDATE ds=10, birthday=?, age=18"
    val insertColumns = SQLUtils.parsePlaceholder(insert)
    assert("name,age,birthday".equals(insertColumns.mkString(",")), "insert语句解析异常")

    val update = "update user set name=?,age=?,is_delete=0 where ds=? and id=? and time=10"
    val updateColumns = SQLUtils.parsePlaceholder(update)
    assert("name,age,ds,id".equals(updateColumns.mkString(",")), "update语句解析异常")

    val delete = "delete from user where id=? and name=? and age=10"
    val deleteColumns = SQLUtils.parsePlaceholder(delete)
    assert("id,name".equals(deleteColumns.mkString(",")), "delete语句解析异常")

    val replace = "replace into users (id,name,age) values(123, ?, ?)"
    val replaceColumns = SQLUtils.parsePlaceholder(replace)
    assert("name,age".equals(replaceColumns.mkString(",")), "replace语句解析异常")

    val merge =
      """
        |merge into zto_hw_board_server a
        |  using (select ? biz_no,
        |                ? appeal_no,
        |                ? postal_bill_code,
        |                ? postal_category_code,
        |                to_date(?, 'yyyy-mm-dd:hh24:mi:ss') postal_create_time,
        |                sysdate gmt_create_time,
        |                sysdate gmt_modify_time
        |         from dual) b
        |  on (a.biz_no = b.biz_no)
        |  when matched then
        |    update set
        |       a.appeal_no = ?,
        |       a.postal_category_code = b.postal_category_code,
        |       a.postal_bill_code = (case when b.postal_bill_code = ' ' then a.postal_bill_code else b.postal_bill_code end),
        |       a.postal_create_time = ?,
        |       a.gmt_modify_time = b.gmt_modify_time
        |  when not matched then
        |    insert (biz_no,appeal_no,postal_bill_code,postal_category_code,postal_create_time,gmt_create_time,gmt_modify_time)
        |    values(?,?,b.postal_bill_code,?,b.postal_create_time,b.gmt_modify_time,b.gmt_modify_time)
        |""".stripMargin

    val mergeColumns = SQLUtils.parsePlaceholder(merge)
    assert("biz_no,appeal_no,postal_bill_code,postal_category_code,postal_create_time,appeal_no,postal_create_time,biz_no,appeal_no,postal_category_code".equals(mergeColumns.mkString(",")), "merge语句解析异常")
  }

  @Test
  def testSelectParse(): Unit = {
    var sql = "select batchNumber from rt_zto_truck_p2p"
    var jdbcLineages = SQLUtils.parseLineage(sql)
    assert("rt_zto_truck_p2p".equals(jdbcLineages.map(_._1).mkString(",")), "基础select语句解析异常")

    //子查询
    sql = "select  batchNumber  from ( select t.batch_number as batchNumber from rt_zto_truck_p2p t where t.gmt_modified >= '2025-04-21 13:21:00' and t.gmt_modified < '2025-04-21 13:24:00' and  deletionstatecode=0 ) t where t.flag=0"
    jdbcLineages = SQLUtils.parseLineage(sql)
    assert("rt_zto_truck_p2p".equals(jdbcLineages.map(_._1).mkString(",")), "子查询select语句解析异常")

    //常规join
    sql = "select a.id,b.id from a left join b on a.id = b.id"
    jdbcLineages = SQLUtils.parseLineage(sql)
    assert("a,b".equals(jdbcLineages.map(_._1).mkString(",")), "基础join语句解析异常")

    //子查询join
    sql = "select a.id,b.id from a left join (select * from b) bb on a.id = b.id"
    jdbcLineages = SQLUtils.parseLineage(sql)
    assert("a,b".equals(jdbcLineages.map(_._1).mkString(",")), "子查询join语句解析异常")

    //多层子查询
    sql = "select *from (select *from (select a.id from a) aa) aaa left join  (select *from (select b.id from b) bb) bbb on aaa.id = bbb.id"
    jdbcLineages = SQLUtils.parseLineage(sql)
    assert("a,b".equals(jdbcLineages.map(_._1).mkString(",")), "多层子查询语句解析异常")

    //线上用例
    sql = "select  batchNumber,         unlaodTransferId,         unloadCenterId,         date_format(estimateDate , '%Y-%m-%d %h:%i:%s')   estimateDate,         date_format(actualEndDate , '%Y-%m-%d %h:%i:%s')  actualEndDate,         dispNum,         dispWeight,         transferInProNum,         if(transferInProNum=0,0,transferInProWeight) as transferInProWeight,         outProNum,         outProWeight,         publishDate  from ( select t.batch_number as batchNumber,        t.end_site_id  as unlaodTransferId,        t.end_center_id as unloadCenterId,        case when estimate_end_date<='1999-01-01 00:00:00'             then gps_arrive_date             else estimate_end_date             end as estimateDate ,        t.actual_end_date as actualEndDate,        t.disp_bills + t.disp_bag_bills as dispNum,        t.disp_weight as dispWeight,        (t.in_pro_num - t.disp_bills - t.disp_bag_bills) as transferInProNum,        t.in_pro_weight - t.disp_weight as transferInProWeight,        if((t.disp_bills + t.disp_bag_bills)>0 and disp_weight=0,1,0) as flag,        t.out_pro_num as outProNum,        t.out_pro_weight as outProWeight,        '2025-04-21 13:24:00' as publishDate from rt_zto_truck_p2p t where t.gmt_modified >= '2025-04-21 13:21:00' and t.gmt_modified < '2025-04-21 13:24:00' and  deletionstatecode=0 ) t where t.flag=0"
    jdbcLineages = SQLUtils.parseLineage(sql)
    assert("rt_zto_truck_p2p".equals(jdbcLineages.map(_._1).mkString(",")), "线上用例解析异常")


  }
}
