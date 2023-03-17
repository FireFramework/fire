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

package com.zto.fire.hudi.conf

import com.zto.fire.common.conf.KeyNum
import com.zto.fire.common.util.PropUtils

/**
 * Hudi相关配置
 *
 * @author ChengLong 2023-03-15 17:06:40
 * @since 2.3.5
 */
private[fire] object FireHudiConf {
  lazy val HUDI_FORMAT = "org.apache.hudi"
  lazy val HUDI_OPTIONS_START = "hudi.options."

  // Spark write hudi的options选项
  def hudiOptions(keyNum: Int = KeyNum._1): Map[String, String] = PropUtils.sliceKeysByNum(this.HUDI_OPTIONS_START, keyNum)
}
