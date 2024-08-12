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

package org.apache.flink.connector.paimon

import com.zto.fire.flink.FlinkStreaming
import org.apache.flink.connector.paimon.conf.FirePaimonConf
import org.apache.flink.table.api.SqlDialect

/**
 * Paimon通用父类，会自动初始化paimon catalog
 *
 * @author ChengLong
 * @Date 2024/8/2 10:27
 * @version 2.3.5
 */
trait BasePaimonStreaming extends FlinkStreaming {

  /**
   * 初始化paimon catalog
   */
  override protected def preProcess(): Unit = {
    val paimonCatalog = s"""
                           |CREATE CATALOG ${FirePaimonConf.paimonCatalogName} WITH (
                           |  'type' = 'paimon',
                           |  'metastore' = 'hive',
                           |  'uri' = '${FirePaimonConf.getMetastoreUrl}'
                           |)
                           |""".stripMargin
    logInfo(
      s"""
        |execute create paimon catalog statement:
        |$paimonCatalog
        |""".stripMargin)
    sql(paimonCatalog)
  }

  /**
   * 使用paimon catalog
   */
  protected def usePaimonCatalog(catalogName: String = FirePaimonConf.paimonCatalogName): Unit = {
    this.tableEnv.useCatalog(FirePaimonConf.paimonCatalogName)
    this.tableEnv.getConfig.setSqlDialect(SqlDialect.DEFAULT)
  }
}
