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

package com.zto.fire.spark.ext.core

import org.apache.spark.SparkContext

/**
  * SparkContext扩展
  *
  * @param sc
  * SparkContext对象
  * @author ChengLong 2019-5-18 10:53:56
  */
class SparkContextExt(sc: SparkContext) {

  /**
   * 判断SparkContext是否已启动
   *
   * @return
   * true：Spark上下文初始化完成 false:已销毁
   */
  def isStarted: Boolean = {
    if (sc == null) return false
    !sc.isStopped
  }
}