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

package com.zto.fire.examples.flink

import com.zto.fire._
import com.zto.fire.common.conf.FireHiveConf
import com.zto.fire.common.util.{DatasourceManager, JSONUtils, PropUtils, ReflectionUtils, StringsUtils}
import com.zto.fire.examples.bean.Student
import com.zto.fire.flink.BaseFlinkStreaming
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.scala._
import org.apache.flink.table.api.SqlDialect
import org.apache.flink.table.api.internal.TableEnvironmentImpl
import org.apache.flink.table.catalog.ObjectPath

/**
 * 基于fire框架进行Flink SQL开发<br/>
 * <a href='https://www.bookstack.cn/read/ApacheFlink-1.12-zh/4b9179fa090b9360.md'>1. Flink SQL开发官方文档——kafka connector</a><br/>
 * <a href='https://www.bookstack.cn/read/ApacheFlink-1.12-zh/a7dfbfd1c259be68.md'>2. Flink SQL开发官方文档——jdbc connector</a>
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2021-01-18 17:24
 */
object Test extends BaseFlinkStreaming {

  // 用于模拟生成kafka中的消息
  """
    |{"table":"t_student","before":{"id":1,"age":1,"name":"spark1","length":51.1,"createTime":"2021-06-20 11:31:51"},"after":{"id":1,"age":21,"name":"flink1","length":151.1,"createTime":"2021-06-22 10:31:30"}}
    |{"table":"t_student","before":{"id":2,"age":2,"name":"spark2","length":52.2,"createTime":"2021-06-20 11:32:52"},"after":{"id":2,"age":22,"name":"flink2","length":152.2,"createTime":"2021-06-22 10:32:30"}}
    |{"table":"t_student","before":{"id":3,"age":3,"name":"spark3","length":53.3,"createTime":"2021-06-20 11:33:53"},"after":{"id":3,"age":23,"name":"flink3","length":153.3,"createTime":"2021-06-22 10:33:30"}}
    |{"table":"t_student","before":{"id":4,"age":4,"name":"spark4","length":54.4,"createTime":"2021-06-20 11:34:54"},"after":{"id":4,"age":24,"name":"flink4","length":154.4,"createTime":"2021-06-22 10:34:30"}}
    |{"table":"t_student","before":{"id":5,"age":5,"name":"spark5","length":55.5,"createTime":"2021-06-20 11:35:55"},"after":{"id":5,"age":25,"name":"flink5","length":155.5,"createTime":"2021-06-22 09:35:30"}}
    |""".stripMargin

  // 具体的业务逻辑放到process方法中
  override def process: Unit = {
    // 创建kafka源表，用于消费kafka消息
    this.fire.sql(
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

    // 创建映射kafka源表的视图表，方便后续取用数据
    this.fire.sql(
      """
        |create view v_student as
        |select
        |	t.`table` as table_name,
        |	after.id as id,
        |	after.age as age,
        |	after.name as name,
        |	after.length as length,
        |	after.createTime as create_time
        |from t_student t
        |""".stripMargin)

    // 创建sink表，将分析后的数据写入到关系型数据库
    this.fire.sql(
      """
        |CREATE TABLE sink (
        |  id BIGINT,
        |  name STRING,
        |  age INT,
        |  `count` bigint,
        |  PRIMARY KEY (id) NOT ENFORCED
        |) WITH (
        |   'connector' = 'jdbc',
        |   'url' = 'jdbc:mysql://10.9.46.116:3306/fire',
        |   'table-name' = 'flink_sql_test',
        |   'driver' = 'com.mysql.jdbc.Driver',
        |   'username' = 'root',
        |   'password' = '..root726',
        |   'sink.buffer-flush.interval' = '10s',
        |   'sink.buffer-flush.max-rows' = '3',
        |   'sink.max-retries' = '3'
        |)
        |""".stripMargin)

    // 将分析后的数据写入到关系型数据库中
    this.fire.sql(
      """
        |insert into sink
        |select id, name, age, sum(1) as `count`
        |from v_student
        |group by id,name,age
        |""".stripMargin)
  }

  /**
   * 初始化flink上下文，新版本fire框架将不再需要用户手动编写初始化代码以及main方法，直接在process方法中编写业务逻辑代码即可
   */
  override def main(args: Array[String]): Unit = {
    this.init()
  }
}
