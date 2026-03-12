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

import com.zto.fire.common.enu.ThreadPoolType
import org.junit.Test

import java.util.concurrent.{CountDownLatch, ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

/**
 * ThreadUtils单元测试
 *
 * @author ChengLong 2026-03-11 14:12:42
 */
class ThreadUtilsTest {
  case class TaskInfo(name: String, sleepMs: Int)

  @Test
  def testRunAsSingle(): Unit = {
    val latch = new CountDownLatch(1)
    val threadName = new AtomicReference[String]()
    ThreadUtils.runAsSingle {
      threadName.set(Thread.currentThread().getName)
      latch.countDown()
    }
    assert(latch.await(3, TimeUnit.SECONDS), "runAsSingle任务未按预期执行")
    assert(threadName.get() != null && threadName.get().nonEmpty, "runAsSingle未获取到线程名")
  }

  @Test
  def testRun(): Unit = {
    val latch = new CountDownLatch(1)
    val threadName = new AtomicReference[String]()
    ThreadUtils.run {
      threadName.set(Thread.currentThread().getName)
      latch.countDown()
    }
    assert(latch.await(3, TimeUnit.SECONDS), "run任务未按预期执行")
    assert(threadName.get() != null && threadName.get().nonEmpty, "run未获取到线程名")
  }

  @Test
  def testCreateManagedThreadPoolAndShutdownByName(): Unit = {
    val poolName = s"ThreadUtilsManagedPoolTest_${System.nanoTime()}"
    val pool1 = ThreadUtils.createManagedThreadPool(poolName, ThreadPoolType.FIXED, 1)
    val pool2 = ThreadUtils.createManagedThreadPool(poolName, ThreadPoolType.FIXED, 5)
    assert(pool1 eq pool2, "同名managed线程池应复用同一个实例")

    ThreadUtils.shutdown(poolName)
    assert(pool1.isShutdown, "shutdown(poolName)后线程池应为关闭状态")

    val pool3 = ThreadUtils.createManagedThreadPool(poolName, ThreadPoolType.FIXED, 1)
    assert(!(pool1 eq pool3), "shutdown(poolName)后应允许同名线程池重建")
    ThreadUtils.shutdown(poolName)
  }

  @Test
  def testCreateThreadPoolAndShutdownByExecutor(): Unit = {
    val pool = ThreadUtils.createThreadPool(ThreadPoolType.FIXED, 2)
    val latch = new CountDownLatch(1)
    pool.execute(new Runnable {
      override def run(): Unit = latch.countDown()
    })

    assert(latch.await(3, TimeUnit.SECONDS), "createThreadPool创建的线程池未执行任务")
    ThreadUtils.shutdown(pool)
    assert(pool.isShutdown, "shutdown(pool)后线程池应为关闭状态")
  }

  @Test
  def testParallelProcessWithThreadNum(): Unit = {
    val tasks = Seq(
      TaskInfo("Task A", 3),
      TaskInfo("Task B", 10),
      TaskInfo("Task C", 5),
      TaskInfo("Task D", 5),
      TaskInfo("Task E", 8)
    )

    val resultList = ThreadUtils.parallelProcess[TaskInfo, String](tasks, threadNum = 3)(partition => {
      val sb = new StringBuilder()
      partition.foreach(taskInfo => {
        sb.append(taskInfo.name).append(",")
        Thread.sleep(taskInfo.sleepMs * 1000)
      })
      sb.toString()
    })

    val allNames = resultList.mkString("")
    assert(resultList.size == 3, "按3线程分组后应得到3个分片结果")
    assert(tasks.forall(task => allNames.contains(task.name)), "parallelProcess(threadNum)应覆盖所有任务")
  }

  @Test
  def testParallelProcessWithThreadPoolExecutor(): Unit = {
    val executor = ThreadUtils.createThreadPool(ThreadPoolType.FIXED, 3).asInstanceOf[ThreadPoolExecutor]
    try {
      val tasks = Seq(
        TaskInfo("Task A", 3),
        TaskInfo("Task B", 10),
        TaskInfo("Task C", 5),
        TaskInfo("Task D", 5),
        TaskInfo("Task E", 8)
      )
      val resultList = ThreadUtils.parallelProcess[TaskInfo, String](tasks, executor)(partition => {
        val sb = new StringBuilder()
        partition.foreach(taskInfo => {
          sb.append(taskInfo.name).append(",")
          Thread.sleep(taskInfo.sleepMs * 1000)
        })
        sb.toString()
      })
      val allNames = resultList.mkString("")
      assert(resultList.size == 3, "parallelProcess(executor)应按线程池并发度分组")
      assert(tasks.forall(task => allNames.contains(task.name)), "parallelProcess(executor)应覆盖所有任务")
    } finally {
      ThreadUtils.shutdown(executor)
    }
  }

  @Test
  def testParallelProcessWithExecutor(): Unit = {
    val executor = ThreadUtils.createThreadPool(ThreadPoolType.FIXED, 2)
    try {
      val tasks = Seq(
        TaskInfo("Task A", 2),
        TaskInfo("Task B", 10),
        TaskInfo("Task C", 6),
        TaskInfo("Task D", 5)
      )
      val resultList = ThreadUtils.parallelProcess[TaskInfo, String](tasks, threadNum = 2, executor)(partition => {
        val sb = new StringBuilder()
        partition.foreach(taskInfo => {
          sb.append(taskInfo.name).append(",")
          Thread.sleep(taskInfo.sleepMs * 1000)
        })
        sb.toString()
      })
      val allNames = resultList.mkString("")
      assert(resultList.size == 2, "parallelProcess(threadNum, executor)分组数不符合预期")
      assert(tasks.forall(task => allNames.contains(task.name)), "parallelProcess(threadNum, executor)应覆盖所有任务")
    } finally {
      ThreadUtils.shutdown(executor.asInstanceOf[java.util.concurrent.ExecutorService])
    }
  }

  @Test
  def testParallelProcessEachWithThreadNum(): Unit = {
    val tasks = Seq(
      TaskInfo("Task A", 5),
      TaskInfo("Task B", 10),
      TaskInfo("Task C", 3),
      TaskInfo("Task D", 8)
    )
    val resultList = ThreadUtils.parallelProcessEach[TaskInfo, String](tasks, threadNum = 3)(taskInfo => {
      Thread.sleep(taskInfo.sleepMs * 1000)
      s"${taskInfo.name} finished"
    })
    assert(resultList.size == tasks.size, "parallelProcessEach(threadNum)结果数量应与输入一致")
    assert(resultList.zip(tasks).forall(t => t._1.startsWith(t._2.name)), "parallelProcessEach(threadNum)应保持输入顺序")
  }

  @Test
  def testParallelProcessEachWithExecutor(): Unit = {
    val executor = ThreadUtils.createThreadPool(ThreadPoolType.FIXED, 3)
    try {
      val tasks = Seq(
        TaskInfo("Task A", 0),
        TaskInfo("Task B", 10),
        TaskInfo("Task C", 5)
      )
      val resultList = ThreadUtils.parallelProcessEach[TaskInfo, String](tasks, executor)(taskInfo => {
        Thread.sleep(taskInfo.sleepMs * 1000)
        taskInfo.name
      })
      assert(resultList == tasks.map(_.name), "parallelProcessEach(executor)应按输入顺序返回结果")
    } finally {
      ThreadUtils.shutdown(executor.asInstanceOf[java.util.concurrent.ExecutorService])
    }
  }

  @Test
  def testParallelProcessEachWithExecutor2(): Unit = {
    val executor = ThreadUtils.createThreadPool(ThreadPoolType.FIXED, 6)
    try {
      val start = System.currentTimeMillis()
      val tasks = Seq(
        TaskInfo("Task A", 3),
        TaskInfo("Task B", 10),
        TaskInfo("Task C", 5),
        TaskInfo("Task D", 8),
        TaskInfo("Task E", 6),
        TaskInfo("Task F", 9)
      )
      val resultList = ThreadUtils.parallelProcessEach[TaskInfo, String](tasks, executor)(taskInfo => {
        Thread.sleep(taskInfo.sleepMs * 1000)
        taskInfo.name
      })
      assert(resultList == tasks.map(_.name), "parallelProcessEach(executor)应按输入顺序返回结果")
      val end = System.currentTimeMillis() - start
      println(s"总耗时：${end}")
      assert(end < ((tasks.map(t => t.sleepMs * 1000).max + 1000)), "")
    } finally {
      ThreadUtils.shutdown(executor)
    }
  }

  @Test
  def testParallelProcessEachWithThreadPoolExecutor(): Unit = {
    val executor = ThreadUtils.createThreadPool(ThreadPoolType.FIXED, 2).asInstanceOf[ThreadPoolExecutor]
    try {
      val tasks = Seq(
        TaskInfo("Task A", 5),
        TaskInfo("Task B", 8)
      )
      val resultList = ThreadUtils.parallelProcessEach[TaskInfo, String](tasks, executor)(taskInfo => {
        Thread.sleep(taskInfo.sleepMs * 1000)
        taskInfo.name
      })
      assert(resultList == Seq("Task A", "Task B"), "parallelProcessEach(ThreadPoolExecutor)结果不符合预期")
    } finally {
      ThreadUtils.shutdown(executor)
    }
  }

  @Test
  def testParallelProcessWithThreadNum2(): Unit = {
    val tasks = Seq(
      TaskInfo("Task A", 3),
      TaskInfo("Task B", 10),
      TaskInfo("Task C", 5),
      TaskInfo("Task D", 5),
      TaskInfo("Task E", 8)
    )

    // 线程池足够的情况下执行耗时就是最大耗时的任务时间
    val start1 = System.currentTimeMillis()
    val resultList = ThreadUtils.parallelProcess[TaskInfo, String](tasks, threadNum = 5)(partition => {
      val sb = new StringBuilder()
      partition.foreach(taskInfo => {
        sb.append(taskInfo.name).append(",")
        Thread.sleep(taskInfo.sleepMs * 1000)
      })
      sb.toString()
    })

    val allNames = resultList.mkString("")
    assert(resultList.size == 5, "按5线程分组后应得到5个分片结果")
    assert(tasks.forall(task => allNames.contains(task.name)), "parallelProcess(threadNum)应覆盖所有任务")

    val end1 = System.currentTimeMillis()
    println(s"step1 执行耗时：${end1 - start1}")
    assert((end1 - start1) <= (tasks.map(t => t.sleepMs).max * 1000 + 1000), "线程池大于等于集合数的时候，执行时间一定小于睡眠最大值+1s")
  }

  @Test
  def testParallelProcessWithThreadNum3(): Unit = {
    val tasks = Seq(
      TaskInfo("Task A", 3),
      TaskInfo("Task D", 5)
    )

    // 线程池足够的情况下执行耗时就是最大耗时的任务时间
    val start1 = System.currentTimeMillis()
    val resultList = ThreadUtils.parallelProcess[TaskInfo, String](tasks, threadNum = 1)(partition => {
      val sb = new StringBuilder()
      partition.foreach(taskInfo => {
        sb.append(taskInfo.name).append(",")
        Thread.sleep(taskInfo.sleepMs * 1000)
      })
      sb.toString()
    })

    val allNames = resultList.mkString("")
    assert(resultList.size == 1, "按1线程分组后应得到1个分片结果")
    assert(tasks.forall(task => allNames.contains(task.name)), "parallelProcess(threadNum)应覆盖所有任务")

    val end1 = System.currentTimeMillis()
    println(s"step1 执行耗时：${end1 - start1}")
    assert((end1 - start1) > (tasks.map(t => t.sleepMs).sum * 1000), "线程池大于等于集合数的时候，执行时间一定大于睡眠总和")
  }

  @Test
  def testParallelProcessWithThreadNum4(): Unit = {
    val tasks = Seq(
      TaskInfo("Task A", 3),
      TaskInfo("Task B", 10),
      TaskInfo("Task C", 5),
      TaskInfo("Task D", 5),
      TaskInfo("Task E", 8)
    )

    // 线程池足够的情况下执行耗时就是最大耗时的任务时间
    val start1 = System.currentTimeMillis()
    val resultList = ThreadUtils.parallelProcess[TaskInfo, String](tasks, threadNum = 2)(partition => {
      val sb = new StringBuilder()
      partition.foreach(taskInfo => {
        sb.append(taskInfo.name).append(",")
        Thread.sleep(taskInfo.sleepMs * 1000)
      })
      sb.toString()
    })

    val allNames = resultList.mkString("")
    assert(resultList.size == 2, "按5线程分组后应得到2个分片结果")
    assert(tasks.forall(task => allNames.contains(task.name)), "parallelProcess(threadNum)应覆盖所有任务")

    val end1 = System.currentTimeMillis()
    println(s"step1 执行耗时：${end1 - start1}")
    // 执行耗时最长的情况是两个最耗时的任务分到了同一个组中（同一个线程处理）
    assert((end1 - start1) <= (10+8) * 1000 + 1000, "线程池为2时，执行时间一定小于等于两个最耗时的任务之和+1s")
  }
}

