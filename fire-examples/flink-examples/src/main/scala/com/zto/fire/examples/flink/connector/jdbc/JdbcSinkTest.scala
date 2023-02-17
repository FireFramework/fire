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

package com.zto.fire.examples.flink.connector.jdbc

import com.zto.fire._
import com.zto.fire.common.enu.Datasource
import com.zto.fire.common.util.{DateFormatUtils, JSONUtils}
import com.zto.fire.core.anno.connector._
import com.zto.fire.core.anno.lifecycle.Process
import com.zto.fire.examples.bean.Student
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming
import org.apache.flink.api.scala._

@Streaming(30)
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")
@Jdbc(url = "jdbc:mysql://mysql-server:3306/fire", username = "root", password = "root")
object JdbcSinkTest extends FlinkStreaming {

  @Process
  def kafkaSource: Unit = {
    // 执行单个查询，结果集直接封装到Student类的对象中，该api自动从指定的keyNum获取对应的数据源信息
    val students = this.fire.jdbcQueryList[Student]("select * from spark_test where age>?", Seq(1))
    println("总计：" + students.length)

    // 执行update、delete、insert、replace、merge等语句
    this.fire.jdbcUpdate("delete from spark_test where age>?", Seq(10), keyNum = 1)

    val dstream = this.fire.createKafkaDirectStream().map(t => JSONUtils.parseObject[Student](t))
    val sql =
      s"""
         |insert into spark_test(name, age, createTime) values(?, ?, '${DateFormatUtils.formatCurrentDateTime()}')
         |ON DUPLICATE KEY UPDATE age=18
         |""".stripMargin
    // 1. 将数据实时写入到@Jdbc指定的数据源，无需指定driverclass
    // 2. sinkJdbc只需指定sql语句即可，fire会自动推断sql中占位符与JavaBean中成员变量的对应关系，并自动设置到PreparedStatement中
    // 3. 支持update、delete、replace、merge、insert等语句
    // 4. 支持自动将下划线命名的字段与JavaBean中驼峰式命名的成员变量自动映射
    // 5. 如果是将数据写入其他数据源，可通过keyNum=xxx指定：
    //    dstream.sinkJdbc(sql, keyNum=3)表示将数据写入@Jdbc3所配置的数据源中
    dstream.sinkJdbc(sql)

    // sinkJdbcExactlyOnce支持仅一次的语义，默认支持mysql，如果是Oracle或PostgreSQL，可通过参数指定：
    // dstream.sinkJdbcExactlyOnce(sql, dbType = Datasource.ORACLE, keyNum=2)
    dstream.sinkJdbcExactlyOnce(sql, keyNum=2)
  }
}