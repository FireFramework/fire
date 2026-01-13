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

import com.zto.fire.spark.SparkCore

/**
 * Paimon 离线Spark Core任务父类
 *
 * @author ChengLong
 * @Date 2025/7/8 14:01
 * @version 2.6.0
 */
trait BasePaimonCore extends SparkCore with BasePaimon {

  /**
   * 在加载任务配置文件前将被加载
   */
  override private[fire] def loadConf: Unit = {
    super.loadConf
    this.loadPaimonConf
  }

  /**
   * 在加载用户配置以后加载hms地址
   */
  override protected def preProcess(): Unit = {
    this.loadHmsConf
  }
}
