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
import com.zto.fire.common.util.{DateFormatUtils, PropUtils}
import com.zto.fire.hudi.enu.HoodieOperationType
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
    // 执行前置SQL
    val ddl = this.sqlCreate(this.tableName)
    if (noEmpty(ddl)) {
      logInfo(s"开始执行hudi前置SQL：\n $ddl")
      sql(ddl)
    }

    this.fire.createMQStream(rdd => {
      if (!rdd.isEmpty()) {
        val cachedRDD = rdd.cache()
        logger.info(s"当前批次记录数（${DateFormatUtils.formatCurrentDateTime()}）：" + cachedRDD.count())
        if (this.sink) sinkHudi(cachedRDD)
        cachedRDD.uncache
      }
    })
  }

  /**
   * 将转换后的消息数据集插入到指定的hudi表中
   */
  def sinkHudi(rdd: RDD[String]): Unit = {
    // 1. 解析消息中的json
    import fire.implicits._
    val rddMsg = if (repartition > 0) rdd.toDS().repartition(repartition) else rdd.toDS()
    val df = this.fire.read.json(rddMsg)

    // 2. 将解析后的json注册为临时表
    df.createOrReplaceTempView(this.tmpView)

    // 3. 将用户传入的查询sql结果集写入到指定的hudi表中
    val inputDF = sql(sqlUpsert(this.tmpView)).cache()
    inputDF.sinkHudi(this.tableName, this.primaryKey, this.precombineKey, this.partitionFieldName)

    // 4. 执行delete语句进行数据删除
    val sqlDelete = this.sqlDelete(this.tmpView)
    if (noEmpty(sqlDelete)) {
      logInfo(s"开始执行hudi删除逻辑：\n $sqlDelete")
      val deleteDF = sql(sqlDelete).cache()
      // 此处不使用deleteDF.isEmpty是基于性能考虑，count会触发cache
      if (deleteDF.count() > 0) {
        deleteDF.sinkHudi(this.tableName, this.primaryKey, this.precombineKey, this.partitionFieldName, operationType = HoodieOperationType.DELETE)
      }
    }

    this.fire.uncache(inputDF)
  }
}
