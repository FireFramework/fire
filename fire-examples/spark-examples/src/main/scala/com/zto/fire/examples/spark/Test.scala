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

package com.zto.fire.examples.spark

import com.zto.fire._
import com.zto.fire.core.anno.connector._
import com.zto.fire.examples.bean.Student
import com.zto.fire.spark.SparkStreaming
import com.zto.fire.spark.anno.Streaming
import com.zto.fire.spark.connector.StreamingConnectors.JSONConnector
import org.aspectj.lang.JoinPoint
import org.aspectj.lang.annotation.{Aspect, Before}

@Aspect
class Test {

  @Before("execution(* org.apache.spark.sql.SparkSession.sql(java.lang.String)) && args(str)")
  def setStartTimeInThreadLocal(joinPoint: JoinPoint, str: String): Unit = {
    println("before ..." + str)
    //println("sql=" + sqlRaw)
  }

  /*@Around("execution(public org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> org.apache.spark.sql.SparkSession.sql(java.lang.String)) && args(sqlRaw)")
  def around(pjp: ProceedingJoinPoint, sqlRaw: String): Dataset[Row] = {
    val sql = sqlRaw.trim
    val spark = pjp.getThis.asInstanceOf[SparkSession]
    val userId = spark.sparkContext.sparkUser
    val tables = getTables(sql, spark)
    tables.foreach(x => println("table=" + x))
    if (accessControl(userId, tables)) {
      pjp.proceed(pjp.getArgs).asInstanceOf[Dataset[Row]]
    } else {
      throw new IllegalAccessException("access failed")
    }
  }

  def getTables(query: String,
                spark: SparkSession): Seq[String] = {
    val logicalPlan = spark.sessionState.sqlParser.parsePlan(query)
    import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
    logicalPlan.collect { case r: UnresolvedRelation => r.tableName }
  }

  def accessControl(user: String,
                    table: Seq[String]): Boolean = {
    println("userId: " + user, "\n tableName: " + table.mkString(","))
    true
  }*/
}

/**
 * 基于Fire进行Spark Streaming开发
 *
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@HBase("test")
@Hive("fat")
@Streaming(interval = 10)
object Test extends SparkStreaming {

  override def process: Unit = {
    val dstream = this.fire.receiverStream(new JSONConnector[Student](100))

    dstream.foreachRDD(rdd => {
      rdd.foreach(x => println(x))
    })
  }
}
