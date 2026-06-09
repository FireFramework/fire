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

package com.zto.fire.examples.flink.trace

import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.core.anno.lifecycle.Process
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming

import java.sql.DriverManager

@Streaming(30)
@Config(
  """
    |fire.trace.codeStandard.send.mq.url=bigdata_test
    |fire.trace.codeStandard.send.mq.topic=sjzn_platform_realtime_codeStandard
    |""")
object TraceStandardTest extends FlinkStreaming {
  val username = "root"
  val password = "root"
  val url = "jdbc:mysql://mysql-server:3306/fire2?useSSL=true"
  val driver = "com.mysql.jdbc.Driver"

  @Process
  def kafkaSource: Unit = {
    val dstream = this.fire.createRandomIntStream(1)

    dstream.addSink(t => {
      Class.forName("com.mysql.cj.jdbc.Driver")
      val connection = DriverManager.getConnection(url, username, password)
      val statement = connection.prepareStatement("select * from spark_test where age>=0")
      val resultSet = statement.executeQuery()
      while (resultSet.next) {
        val id = resultSet.getInt("id")
        val name = resultSet.getString("name")
        System.out.println("ID: " + id + ", Name: " + name)
      }
      resultSet.close()
      statement.close()
      connection.close()
    })
  }
}