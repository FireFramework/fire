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


/**
 * Spark Streaming通用父接口
 * Created by ChengLong on 2018-03-28.
 */
trait BaseSparkStreaming extends AbstractSparkStreaming {

  /**
   * 初始化SparkSession与StreamingContext，默认批次时间为30s
   * 批次时间可通过子类复写main方法实现或通过在配置文件中指定：spark.streaming.batch.duration=30
   */
  def main(args: Array[String]): Unit = {
    val batchDuration = this.conf.getLong("spark.streaming.batch.duration", 10)
    val ck = this.conf.getBoolean("spark.streaming.receiver.writeAheadLog.enable", false)
    this.init(batchDuration, ck, args)
  }
}
