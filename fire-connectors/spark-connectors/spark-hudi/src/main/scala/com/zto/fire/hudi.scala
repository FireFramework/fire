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

package com.zto.fire

import com.zto.fire.common.conf.KeyNum
import com.zto.fire.hudi.conf.FireHudiConf
import com.zto.fire.hudi.util.HudiUtils
import com.zto.fire.spark.sql.SparkSqlUtils
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.config.HoodieWriteConfig.TBL_NAME
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.JavaConversions

/**
 * hudi connector隐式转换
 *
 * @author ChengLong 2023-03-15 17:03:20
 * @since 2.3.5
 */
package object hudi {

  /**
   * DataFrame扩展
   *
   * @param dataFrame
   * dataFrame实例
   */
  implicit class DataFrameExt(dataFrame: DataFrame) {
    private[this] lazy val tablePathMap = new JHashMap[String, String]()

    /**
     * 将DataFrame中的数据写入到指定的hudi表中
     *
     * @param hudiTableName
     * hudi表名
     * @param recordKey
     * 按该字段进行upsert
     * @param precombineKey
     * 根据该字段进行合并
     * @param partition
     * 分区字段
     * @param options
     * 额外的options信息
     */
    def sinkHudi(hudiTableName: String, recordKey: String,
                 precombineKey: String, partition: String,
                 typeType: HoodieTableType = HoodieTableType.MERGE_ON_READ,
                 options: JMap[String, String] = Map.empty[String, String],
                 keyNum: Int = KeyNum._1
                ): Unit = {
      // 配置文件的优先级高于代码
      val confOptions = options ++= FireHudiConf.hudiOptions(keyNum)
      val hudiTablePath = this.tablePathMap.mergeGet(hudiTableName)(SparkSqlUtils.getTablePath(hudiTableName))
      dataFrame.write.format("org.apache.hudi")
        .option(RECORDKEY_FIELD.key, recordKey)      // 分区内根据ID进行更新，有则更新，没有则插入
        .option(PRECOMBINE_FIELD.key, precombineKey) // 当两条id相同的记录做合并时，按createTime字段取最新的
        .option(PARTITIONPATH_FIELD.key, partition)  // 根据该字段进行分区，也就是分区字段
        .option(TBL_NAME.key, hudiTableName)
        .option(HIVE_STYLE_PARTITIONING.key, "true") // 使用hive的分区格式：ds=20230303这种
        .option(TABLE_TYPE.key, typeType.name)
        .options(confOptions)
        .mode(SaveMode.Append)
        .save(hudiTablePath)
    }
  }

  /**
   * SparkContext扩展
   *
   * @param spark
   * sparkSession对象
   */
  implicit class SparkSessionExtBridge(spark: SparkSession) {

    /**
     * 将已hive表为载体的hudi表注册成hudi表
     *
     * @param hiveTableName
     * hive表名
     * @param hudiViewName
     * hudi视图名
     */
    def createOrReplaceHudiTempView(hiveTableName: String, hudiViewName: String): Unit = {
      requireNonEmpty(hiveTableName)("Hive表名不能为空！")
      requireNonEmpty(hudiViewName)("Hudi视图表名不能为空")

      if (!this.spark.catalog.tableExists(hiveTableName)) throw new IllegalArgumentException(s"Hive表名不存在：$hiveTableName")
      val df = this.spark.read.format("org.apache.hudi").load(SparkSqlUtils.getTablePath(hiveTableName))
      df.createOrReplaceTempView(hudiViewName)
    }
  }
}
