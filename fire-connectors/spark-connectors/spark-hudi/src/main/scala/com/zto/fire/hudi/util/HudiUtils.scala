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

package com.zto.fire.hudi.util

import com.zto.fire._
import com.zto.fire.common.bean.TableIdentifier
import com.zto.fire.common.util.Logging
import com.zto.fire.spark.sql.SparkSqlParser
import com.zto.fire.spark.util.{SparkSingletonFactory, SparkUtils}
import com.zto.fire.{JHashMap, JMap, JStringBuilder, noEmpty, requireNonEmpty, tryWithLog}
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.config.HoodieWriteConfig.TBL_NAME
import org.apache.hudi.exception.TableNotFoundException
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * Hudi相关工具类
 *
 * @author ChengLong 2023-03-10 13:05:43
 * @since 2.3.5
 */
object HudiUtils extends Logging {
  private lazy val fire = SparkSingletonFactory.getSparkSession

  /**
   * 获取给定表的schema信息
   *
   * @param tableName
   * 表名（dbName.tableName）
   * @return
   * StructField
   */
  def tableSchema(tableName: String): Array[StructField] = {
    if (!this.fire.tableExists(tableName)) throw new RuntimeException(s"表${tableName}不存在，无法获取schema信息")
    this.fire.table(tableName).schema.fields
  }

  /**
   * 根据表名与where条件生成查询语句
   *
   * @param tableName
   * 表名
   * @param condition
   * where后面的查询条件
   * @return
   * 查询语句
   */
  def generateQuerySQL(tableName: String, condition: String): String = {
    val fieldList = this.tableSchema(tableName).map(field => field.name).mkString(",\n")
    s"""
       |select
       |${fieldList}
       |from $tableName
       |${if (noEmpty(condition)) "where " + condition}
       |""".stripMargin
  }

  /**
   * 根据schema生成hudi建表SQL语句
   *
   * @param srcTableName
   * hive表名或者Spark的临时表名
   * @param hudiTableName
   * 待创建的hudi表名
   * @return
   * SQL DDL语句
   */
  def generateCreateSQL(srcTableName: String, hudiTableName: String, partitionFiledAndType: String): String = {
    requireNonEmpty(srcTableName, hudiTableName)("表名不能为空，请检查！")

    val fields = this.tableSchema(srcTableName)
    requireNonEmpty(fields)(s"表${srcTableName}的字段列表为空！")

    val fieldBuilder = new JStringBuilder

    // 添加建表语句头
    fieldBuilder
      .append("CREATE TABLE IF NOT EXISTS ")
      .append(hudiTableName).append(" (\n")

    // 遍历schema信息，生成字段列表，排除分区字段
    fields.filter(t => !partitionFiledAndType.contains(t.name)).foreach(field => {
      fieldBuilder
        .append("\t")
        .append(field.toDDL)
        .append(",\n")
    })

    val sqlBuilder = new JStringBuilder(fieldBuilder.substring(0, fieldBuilder.length() - 2))
    sqlBuilder.append("\n) ")

    // 添加分区字段
    if (noEmpty(partitionFiledAndType)) {
      sqlBuilder
        .append("PARTITIONED BY (")
        .append(partitionFiledAndType)
        .append(")\n")
    }

    // 添加hudi表相关属性描述信息
    sqlBuilder.append(
      """
        |ROW FORMAT SERDE
        |  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        |STORED AS INPUTFORMAT
        |  'org.apache.hudi.hadoop.HoodieParquetInputFormat'
        |OUTPUTFORMAT
        |  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
        |""".stripMargin)
    sqlBuilder.toString
  }

  /**
   * 根据hive数仓的表名获取在数仓中的hdfs存储路径
   *
   * @param tableName
   * hive/hudi表名
   * @return
   * url path
   */
  def getTablePath(tableName: String): String = {
    if (!this.fire.tableExists(tableName)) {
      throw new TableNotFoundException(s"Hive表${tableName}不存在，请先建表")
    }

    val metadata = this.fire.sessionState.catalog.getTableMetadata(SparkSqlParser.toSparkTableIdentifier(TableIdentifier(tableName)))
    metadata.storage.locationUri.get.toString
  }

  /**
   * 对指定的分区表进行分区修复
   *
   * @param tableName
   * hive表名
   */
  def repairTable(tableName: String): Unit = {
    tryWithLog {
      this.fire.sql(s"MSCK REPAIR TABLE $tableName")
    }(this.logger, s"${tableName}分区修复完成")
  }

  /**
   * 将DataFrame中的数据写入到指定的hudi表中
   *
   * @param inputDF
   * 数据源
   * @param hudiTableName
   * hudi表名
   * @param recordKey
   * 按该字段进行upsert
   * @param precombineKey
   * 根据该字段进行合并
   * @param partition
   * 分区字段
   * @param upsertMode
   * 是否启用upsert模式进行写入
   * @param options
   * 额外的options信息
   */
  def sinkHudi(inputDF: DataFrame, hudiTableName: String, recordKey: String,
               precombineKey: String, partition: String,
               options: JMap[String, String] = hudiOptions()): Unit = {
    val hudiTablePath = HudiUtils.getTablePath(hudiTableName)

    inputDF.write.format("org.apache.hudi")
      .option(RECORDKEY_FIELD.key, recordKey)       // 分区内根据ID进行更新，有则更新，没有则插入
      .option(PRECOMBINE_FIELD.key, precombineKey)  // 当两条id相同的记录做合并时，按createTime字段取最新的
      .option(PARTITIONPATH_FIELD.key, partition)   // 根据该字段进行分区，也就是分区字段
      .option(TBL_NAME.key, hudiTableName)
       // 使用hive的分区格式：ds=20230303这种
      .option(HIVE_STYLE_PARTITIONING.key, "true")
      .option(TABLE_TYPE.key, HoodieTableType.MERGE_ON_READ.name)
      .options(options)
      .mode(SaveMode.Append)
      .save(hudiTablePath)
  }

  /**
   * 流式写入场景下指定几个批次进行一次commit
   * 注：不建议在实时任务重开启，会影响实时任务性能
   *
   * @param batchCount
   * Streaming batch count
   */
  def deltaCommitOptions(batchCount: Int): JMap[String, String] = {
    val options = new JHashMap[String, String]()
    options.put("hoodie.compact.inline", "true")
    options.put("hoodie.compact.inline.max.delta.commits", batchCount.toString)
    options.put("hoodie.fail.on.timeline.archiving", "false")
    options
  }

  /**
   * 用于根据指定配置构建sink hudi表的options配置
   *
   * @param parallelism
   * hoodie.upsert.shuffle.parallelism
   * @param tableType
   * COPY_ON_WRITE or MERGE_ON_READ
   * @return
   */
  def hudiOptions(parallelism: Int = SparkUtils.executorNum * 3,
                  tableType: HoodieTableType = HoodieTableType.MERGE_ON_READ): JMap[String, String] = {

    val options = new JHashMap[String, String]()
    options.put("hoodie.upsert.shuffle.parallelism", s"$parallelism")
    options.put("hoodie.insert.shuffle.parallelism", s"$parallelism")
    options.put("hoodie.upsert.shuffle.parallelism", s"$parallelism")
    options.put("hoodie.bulkinsert.shuffle.parallelism", s"$parallelism")
    options.put("hoodie.delete.shuffle.parallelism", s"$parallelism")

    // 使用hive的分区格式：ds=20230303这种
    options.put(HIVE_STYLE_PARTITIONING.key, "true")
    // 只有MERGE_ON_READ支持upsert语义
    options.put(TABLE_TYPE.key, tableType.name)
    options
  }
}
