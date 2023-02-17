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

package com.zto.fire.spark.sql

import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.common.util.Logging
import com.zto.fire.spark.conf.FireSparkConf
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

/**
 * 通过listener解析SQL血缘（避免当指定spark.sql.extensions导致无法采集血缘的问题）
 *
 * 注：使用listener的缺点是无法采集到完整的SQL语句，好处是当指定了sql extension仍可进行血缘分析
 *
 * @author ChengLong 2022-10-13 13:45:14
 * @since 2.3.2
 */
class SQLParserListener extends QueryExecutionListener with Logging {

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    if (FireFrameworkConf.lineageEnable && FireSparkConf.sparkLineageListenerEnable) {
      SparkSqlParser.sqlParser(qe.logical)
    }
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {

  }
}
