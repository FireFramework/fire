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

package com.zto.fire.spark

import com.zto.fire._
import com.zto.fire.common.util.DateFormatUtils
import org.apache.spark.rdd.RDD

/**
 * 通用的Spark Streaming实时入湖父类
 *
 * @author ChengLong 2023-05-22 09:30:03
 * @since 2.3.5
 */
trait HudiStreaming extends BaseHudiStreaming {

  override def process: Unit = {
    this.validate
    // 1. 执行前置SQL
    val beforeSQL = this.sqlBefore(this.tableName)
    if (noEmpty(beforeSQL)) {
      logInfo(s"开始执行hudi前置SQL：\n $beforeSQL")
      sql(beforeSQL)
    }

    this.fire.createMQStream(rdd => {
      if (!rdd.isEmpty()) {
        // 2. 将当前批次数据实时upsert到指定hudi表
        val cachedRDD = rdd.cache()
        logInfo(s"当前批次记录数（${DateFormatUtils.formatCurrentDateTime()}）：" + cachedRDD.count())
        if (this.sink) sinkHudi(cachedRDD)

        // 3. 执行后置SQL语句，比如delete、update、merge等
        val afterSQL = this.sqlAfter(this.tableName)
        if (noEmpty(afterSQL)) {
          logInfo(s"开始执行hudi后置SQL：\n $afterSQL")
          sql(afterSQL)
        }
        cachedRDD.uncache
      }
    })(reTry = this.retryOnFailure)
  }

  /**
   * 将转换后的消息数据集插入到指定的hudi表中
   */
  protected def sinkHudi(rdd: RDD[String]): Unit = {
    // 1. 解析消息中的json
    import fire.implicits._
    val rddMsg = if (repartition > 0) rdd.toDS().repartition(repartition) else rdd.toDS()
    val df = this.fire.read.json(rddMsg)

    // 2. 将解析后的json注册为临时表
    df.createOrReplaceTempView(this.tmpView)

    // 3. 将用户传入的查询sql结果集写入到指定的hudi表中
    val inputDF = sql(sqlUpsert(this.tmpView)).cache()
    inputDF.sinkHudi(this.tableName, this.primaryKey, this.precombineKey, this.partitionFieldName)
    this.fire.uncache(inputDF)
  }
}
