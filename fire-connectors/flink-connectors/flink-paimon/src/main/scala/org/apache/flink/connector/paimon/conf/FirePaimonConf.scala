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

package org.apache.flink.connector.paimon.conf

import com.zto.fire.common.util.PropUtils

/**
 * paimon相关配置
 *
 * @author ChengLong
 * @since 2.5.0
 * @create 2024-08-02 10:01:01
 */
private[paimon] object FirePaimonConf {
  lazy val HIVE_CLUSTER = "hive.cluster"
  lazy val PAIMON_CATALOG_NAME = "paimon.catalog.name"

  // paimon catalog名称
  lazy val paimonCatalogName = PropUtils.getString(this.PAIMON_CATALOG_NAME, "paimon")
  // hive集群标识（batch/streaming/test）
  lazy val hiveCluster = PropUtils.getString(this.HIVE_CLUSTER, "")
}