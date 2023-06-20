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

package com.zto.fire.common.util

import com.zto.fire.predef._
import java.net.ServerSocket
import java.util.Collections
import java.util.concurrent.{ConcurrentLinkedQueue, Executors, TimeUnit}
import scala.util.Try

/**
 * 端口池管理器，可自动预分配一段区间内的端口，避免被其他应用占用
 * 内置端口定时回收机制，分配后的端口如果被使用方释放掉，会自动回收到端口池中等待下次分配
 *
 * @author ChengLong 2023-06-14 11:00:08
 * @since 2.3.7
 */
class PortPoolManager(portStart: Int, portEnd: Int) extends Logging {
  private lazy val portPoolQueue = new ConcurrentLinkedQueue[ServerSocket]()
  private lazy val portSet = Collections.synchronizedSet(new JHashSet[Int])
  private lazy val portRecoverExecutor = Executors.newScheduledThreadPool(1)
  private lazy val portRange = portStart until portEnd
  private lazy val allocatePortSet = Collections.synchronizedSet(new JHashSet[Int])
  this.rebind

  /**
   * 重绑定portRange中指定的端口区间
   */
  private[this] def rebind: Unit = {
    allocatePortSet.addAll(portRange)

    // 将未被占用的端口回收
    portRecoverExecutor.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        allocatePortSet.foreach(port => {
          val socket = bindPort(port)
          if (socket.isDefined) {
            portSet.add(port)
            portPoolQueue.add(socket.get)
            logInfo(s"${port}已加入端口池，可用端口数：${portPoolQueue.size()}")
          }
        })
      }
    }, 0, 5, TimeUnit.SECONDS)
  }

  /**
   * 返回一个可用的端口号
   */
  def getAvailablePort: Option[Int] = {
    if (this.portPoolQueue.isEmpty) {
      logWarning("端口池已无可用端口分配！！！")
      return None
    }

    val socket = this.portPoolQueue.poll()
    val availablePort = socket.getLocalPort
    this.portSet.remove(availablePort)
    this.allocatePortSet.add(availablePort)
    socket.close()
    logInfo(s"分配端口：$availablePort，可用端口数：${this.portPoolQueue.size()}")
    Some(availablePort)
  }

  /**
   * 获取当前系统可用的随机端口号
   */
  def getRandomPort: Int = OSUtils.getRundomPort

  /**
   * 绑定给定的端口
   *
   * @param port
   * 端口号
   * @return
   * ServerSocket实例
   */
  private[this] def bindPort(port: Int): Option[ServerSocket] = {
    var socket: ServerSocket = null
    val retValue = Try {
      try {
        socket = new ServerSocket(port)
      }
    }

    if (retValue.isSuccess) Some(socket) else None
  }
}

object PortPoolManager {
  def apply(portStart: Int, portEnd: Int) = new PortPoolManager(portStart, portEnd)
}