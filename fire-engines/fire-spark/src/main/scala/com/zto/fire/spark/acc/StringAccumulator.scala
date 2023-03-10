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

package com.zto.fire.spark.acc

import com.zto.fire._
import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.common.util.Logging
import org.apache.spark.util.AccumulatorV2

import java.util.concurrent.ConcurrentLinkedQueue

/**
  * Fire框架String类型累加器
  *
  * @author ChengLong 2022-08-24 14:44:55
  * @since 2.3.2
  */
private[fire] class StringAccumulator extends AccumulatorV2[String, ConcurrentLinkedQueue[String]] with Logging {
  // 用于存放字符串的队列
  private val queue = new ConcurrentLinkedQueue[String]
  // 判断是否打开日志累加器
  protected lazy val isEnable = FireFrameworkConf.accEnable

  /**
    * 判断累加器是否为空
    */
  override def isZero: Boolean = this.queue.size() == 0

  /**
    * 用于复制累加器
    */
  override def copy(): AccumulatorV2[String, ConcurrentLinkedQueue[String]] = {
    val strAcc = new StringAccumulator
    strAcc.value.addAll(this.queue)
    strAcc
  }

  /**
    * driver端执行有效，用于清空累加器
    */
  override def reset(): Unit = this.queue.clear

  /**
    * executor端执行，用于收集日志信息
    *
    * @param log
    * 日志信息
    */
  override def add(str: String): Unit = {
    if (this.isEnable && noEmpty(str)) {
      this.queue.add(str)
      this.clear
    }
  }

  /**
    * executor端向driver端merge累加数据
    *
    * @param other
    * executor端累加结果
    */
  override def merge(other: AccumulatorV2[String, ConcurrentLinkedQueue[String]]): Unit = {
    if (other != null && other.value.size() > 0) {
      this.queue.addAll(other.value)
      other.value.foreach(t => this.queue.add(t))
      this.clear
    }
  }

  /**
    * driver端获取累加器的值
    *
    * @return
    * 收集到的日志信息
    */
  override def value: ConcurrentLinkedQueue[String] = this.queue

  /**
    * 当日志累积量超过maxLogSize所设定的值时清理过期的日志数据
    * 直到达到minLogSize所设定的最小值，防止频繁的进行清理
    */
  def clear: Unit = {
    if (this.queue.size() > FireFrameworkConf.maxLogSize) {
      while (this.queue.size() > FireFrameworkConf.minLogSize) {
        this.queue.poll
      }
    }
  }
}
