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

package com.zto.fire.examples.spark.paimon

import com.zto.fire.core.anno.connector.Hive
import com.zto.fire.spark.connector.paimon.PaimonCore

/**
 * 集成Paimon测试任务进行spark core任务开发
 *
 * @author ChengLong
 * @Date 2025-07-09 13:32:41
 * @version 2.6.0
 */
@Hive("thrift://ip:9083")
object PaimonTest extends PaimonCore  { // extends PaimonStreaming

  override def process(): Unit = {
    sql(
      """
        |use paimon.paimon
        |""".stripMargin)
    sql(
      """
        |show tables
        |""".stripMargin).show
    sql(
      """
        |select * from paimon.paimon.paimon_table_name where ds=xxx
        |""".stripMargin).show
  }
}

