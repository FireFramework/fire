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

package com.zto.fire.spark.connector.paimon

import com.zto.fire.common.conf.{FireFrameworkConf, FireHiveConf}
import com.zto.fire.common.util.PropUtils
import com.zto.fire.spark.connector.paimon.conf.FirePaimonConf

/**
 * Paimon通用父类，提供通用实现
 *
 * @author ChengLong
 * @Date 2025/7/8 13:57
 * @version 2.6.0
 */
trait BasePaimon {

  /**
   * 加载paimon相关配置信息，包括：catalog、sql extensions、metastore等
   */
  protected def loadPaimonConf: Unit = {
    PropUtils.load(FireFrameworkConf.FIRE_PAIMON_COMMON_CONF_FILE)
  }

  /**
   * 根据用户的配置加载hive metastore url
   */
  protected def loadHmsConf: Unit = {
    val url = FireHiveConf.getMetastoreUrl
    PropUtils.setProperty(FirePaimonConf.PAIMON_CATALOG_URI, url)
  }
}
