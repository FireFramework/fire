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

package com.zto.fire.examples.spark.lineage

import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.common.lineage.LineageManager
import com.zto.fire.core.anno.connector.{HBase, Jdbc}
import com.zto.fire.examples.bean.Student
import com.zto.fire.spark.SparkStreaming
import com.zto.fire.spark.anno.Streaming


/**
 * Random流
 *
 * @author ChengLong
 * @Date 2025/12/31 10:46
 * @version 2.3.
 */
@Config(
  """
    |fire.lineage.enable=true
    |fire.lineage.api.enable=true
    |fire.lineage.debug.print=true
    |""")
@HBase("fat")
@Streaming(10)
@Jdbc(url = "jdbc:mysql://mysql-server:3306/fire?useSSL=true", username = "root", password = "root")
object ApiLineageTest extends SparkStreaming {

  override def process(): Unit = {
    val students = this.fire.jdbcQueryList[Student]("select * from spark_test where age>=?", Seq(1))
    println("总计：" + students.length)

    val rowKeys = Seq("1", "2", "3", "5", "6")
    val studentList = this.fire.hbaseGetListAsync2[Student]("fire_test_13", 1, rowKeys)
    studentList.foreach(println)

    val dstream = this.fire.createRandomLongStream(100)
    dstream.print(1)
    LineageManager.show(30)
  }
}
