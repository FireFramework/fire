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

import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.common.enu.ThreadPoolType
import com.zto.fire.predef._
import org.apache.commons.lang3.StringUtils

import java.util.concurrent._
import java.util.function.Supplier


/**
 * 线程相关工具类
 *
 * @author ChengLong 2019-4-25 15:17:55
 */
object ThreadUtils extends Logging {
  // 用于维护使用ThreadUtils创建的线程池对象，并进行统一的关闭
  private lazy val poolMap = new JConcurrentHashMap[String, ExecutorService]()
  private lazy val singlePool = this.createManagedThreadPool("FireSinglePool", ThreadPoolType.SINGLE)
  private lazy val cachedPool = this.createManagedThreadPool("FireCachedPool", ThreadPoolType.CACHED)
  private lazy val scheduledPool = this.createManagedThreadPool("FireScheduledPool", ThreadPoolType.SCHEDULED, FireFrameworkConf.threadPoolSchedulerSize).asInstanceOf[ScheduledExecutorService]
  // 在JVM退出时统一回收ThreadUtils创建并托管的线程池（含executor/taskmanager进程）
  ShutdownHookManager.addShutdownHook()(() => this.shutdown)

  /**
   * 利用SingleThreadExecutor执行给定的函数
   *
   * @param fun
   * 用于指定以多线程方式执行的函数
   */
  def runAsSingle(fun: => Unit): Unit = {
    this.singlePool.execute(new Runnable {
      override def run(): Unit = fun
    })
  }

  /**
   * 利用CachedThreadPool执行给定的函数
   *
   * @param fun
   * 用于指定以多线程方式执行的函数
   */
  def run(fun: => Unit): Unit = {
    this.cachedPool.execute(new Runnable {
      override def run(): Unit = fun
      logDebug(s"Invoke runAsThread as ${Thread.currentThread().getName}.")
    })
  }

  /**
   * 利用CachedThreadPool循环执行给定的函数
   *
   * @param fun
   * 用于指定以多线程方式执行的函数
   * @param delay
   * 循环调用间隔时间（单位s）
   */
  def runLoop(fun: => Unit, delay: Long = 10): Unit = {
    this.cachedPool.execute(new Runnable {
      override def run(): Unit = {
        while (true) {
          fun
          logDebug(s"Loop invoke runAsThreadLoop as ${Thread.currentThread().getName}. Delay is ${delay}s.")
          try {
            Thread.sleep(math.abs(delay * 1000))
          } catch {
            case _: Throwable =>
          }
        }
      }
    })
  }

  /**
   * 利用ScheduledThreadPool定时调度执行给定的函数
   *
   * @param fun
   * 定时执行的任务函数引用
   * @param initialDelay
   * 第一次延迟执行的时长
   * @param period
   * 每隔指定的时长执行一次
   * @param rate
   * true：表示周期性的执行，不受上一个定时任务的约束
   * false：表示当上一次周期性任务执行成功后，period后开始执行
   * @param timeUnit
   * 时间单位，默认分钟
   */
  def schedule(fun: => Unit, initialDelay: Long, period: Long, rate: Boolean = true, timeUnit: TimeUnit = TimeUnit.MINUTES): Unit = {
    if (rate) {
      // 表示周期性的执行，不受上一个定时任务的约束
      this.scheduledPool.scheduleAtFixedRate(new Runnable {
        override def run(): Unit = wrapFun()
      }, initialDelay, period, timeUnit)
    } else {
      // 表示当上一次周期性任务执行成功后，period后开始执行
      this.scheduledPool.scheduleWithFixedDelay(new Runnable {
        override def run(): Unit = wrapFun()
      }, initialDelay, period, timeUnit)
    }

    // 处理传入的函数
    def wrapFun(): Unit = {
      fun
      logDebug(s"Loop invoke runAsSchedule as ${Thread.currentThread().getName}. Delay is ${period}${timeUnit.name()}.")
    }
  }

  /**
   * 表示当上一次周期性任务执行成功后
   * period后开始执行给定的函数fun
   *
   * @param fun
   * 定时执行的任务函数引用
   * @param initialDelay
   * 第一次延迟执行的时长
   * @param period
   * 每隔指定的时长执行一次
   * @param timeUnit
   * 时间单位，默认分钟
   */
  def scheduleAtFixedRate(fun: => Unit, initialDelay: Long, period: Long, timeUnit: TimeUnit = TimeUnit.MINUTES): Unit = {
    this.schedule(fun, initialDelay, period, true, timeUnit)
  }

  /**
   * 表示当上一次周期性任务执行成功后，period后开始执行fun函数
   * 注：受上一个定时任务的影响
   *
   * @param fun
   * 定时执行的任务函数引用
   * @param initialDelay
   * 第一次延迟执行的时长
   * @param period
   * 每隔指定的时长执行一次
   * @param timeUnit
   * 时间单位，默认分钟
   */
  def scheduleWithFixedDelay(fun: => Unit, initialDelay: Long, period: Long, timeUnit: TimeUnit = TimeUnit.MINUTES): Unit = {
    this.schedule(fun, initialDelay, period, false, timeUnit)
  }

  /**
   * 创建一个新的指定大小的调度线程池，该线程池不会被重复创建，名称相同则返回相同的线程池实力
   * 该方法创建的线程池会被纳入fire框架的生命周期管理中，也就是说当spark或flink任务退出后线程池会被自动销毁
   *
   * @param poolName
   * 线程池标识
   * @param poolType
   * 线程池类型
   * @param poolSize
   * 线程池大小
   */
  private[fire] def createManagedThreadPool(poolName: String, poolType: ThreadPoolType = ThreadPoolType.FIXED, poolSize: Int = 1): ExecutorService = {
    require(StringUtils.isNotBlank(poolName), "线程池名称不能为空")
    // this.poolMap.computeIfAbsent(poolName, _ => this.createThreadPoolByType(poolType, poolSize))
    // 兼容scala 2.11语法
    this.poolMap.computeIfAbsent(poolName, new java.util.function.Function[String, ExecutorService] {
      override def apply(name: String): ExecutorService = createThreadPoolByType(poolType, poolSize)
    })
  }

  /**
   * 创建一个新的指定大小的调度线程池，该线程池的销毁需要调用者自行管理
   *
   * @param poolName
   * 线程池标识
   * @param poolType
   * 线程池类型
   * @param poolSize
   * 线程池大小
   */
  def createThreadPool(poolType: ThreadPoolType = ThreadPoolType.FIXED, poolSize: Int = 1): ExecutorService = {
    this.createThreadPoolByType(poolType, poolSize)
  }

  /**
   * 创建一个新的指定大小以及指定类型的调度线程池
   *
   * @param poolName
   * 线程池标识
   * @param poolType
   * 线程池类型
   * @param poolSize
   * 线程池大小
   */
  private[this] def createThreadPoolByType(poolType: ThreadPoolType = ThreadPoolType.FIXED, poolSize: Int = 1): ExecutorService = {
    val threadPool = poolType match {
      case ThreadPoolType.FIXED => Executors.newFixedThreadPool(poolSize)
      case ThreadPoolType.SCHEDULED => Executors.newScheduledThreadPool(poolSize)
      case ThreadPoolType.SINGLE => Executors.newSingleThreadExecutor()
      case ThreadPoolType.CACHED => Executors.newCachedThreadPool()
      case ThreadPoolType.WORK_STEALING => Executors.newWorkStealingPool()
      case _ => Executors.newFixedThreadPool(poolSize)
    }
    threadPool
  }

  /**
   * 用于释放指定的线程池
   * 关闭后会从 poolMap 中移除，避免持有已关闭池的引用，并允许同名线程池再次创建
   *
   * @param poolName
   * 线程池标识
   */
  private[fire] def shutdown(poolName: String): Unit = {
    if (StringUtils.isNotBlank(poolName) && this.poolMap.containsKey(poolName)) {
      val threadPool = this.poolMap.get(poolName)
      if (threadPool != null && !threadPool.isShutdown) {
        threadPool.shutdownNow()
        logDebug(s"关闭线程池：${poolName}")
      }
      this.poolMap.remove(poolName)
    }
  }

  /**
   * 用于释放指定的线程池
   */
  def shutdown(pool: ExecutorService): Unit = {
    if (pool != null && !pool.isShutdown) {
      pool.shutdown()
      logDebug(s"关闭线程池：${pool}")
    }
  }

  /**
   * 用于释放指定的线程池
   */
  def shutdownNow(pool: ExecutorService): Unit = {
    if (pool != null && !pool.isShutdown) {
      pool.shutdownNow()
      logDebug(s"关闭线程池：${pool}")
    }
  }

  /**
   * 用于释放所有线程池
   * 关闭后会清空 poolMap，避免持有已关闭池的引用
   */
  private[fire] def shutdown: Unit = synchronized {
    val poolNum = this.poolMap.size()
    if (this.poolMap.size() > 0) {
      this.poolMap.foreach(pool => {
        if (pool != null && pool._2 != null && !pool._2.isShutdown) {
          pool._2.shutdownNow()
          logInfo(s"---> 完成线程池[ ${pool._1} ]的资源回收. <---")
        }
      })
      this.poolMap.clear()
    }
    logInfo(s"---> 完成所有线程池回收，总计：${poolNum}个. <---")
  }

  /**
   * 对给定的数据集按线程数自动分组，并根据给定的处理逻辑（fun）进行并发处理（每次调用会创建线程池）
   *
   * 调用示例请参考：
   * ThreadUtilsTest.testParallelProcessWithThreadNum
   *
   * @param data
   * 待处理的数据集
   * @param threadNum
   * 线程数
   * @return
   * 按分组顺序返回每个线程处理后的结果集
   */
  def parallelProcess[T, R](data: Seq[T], threadNum: Int)(fun: Seq[T] => R): Seq[R] = {
    if (data == null || data.isEmpty) return Seq.empty[R]

    val executor = Executors.newFixedThreadPool(math.abs(threadNum))
    try {
      this.parallelProcess(data, threadNum, executor)(fun)
    } finally {
      this.shutdown(executor)
    }
  }

  /**
   * 对给定的数据集按 `ThreadPoolExecutor` 的最大线程数自动分组，并根据给定的处理逻辑（fun）进行并发处理
   *
   * 调用示例请参考：
   * ThreadUtilsTest.testParallelProcessWithThreadPoolExecutor
   *
   * @param data
   * 待处理的数据集
   * @param executor
   * 指定的 `ThreadPoolExecutor`，将自动使用其最大线程数作为并发分组数，并复用线程资源
   * @param fun
   * 对每个分组数据的处理逻辑。每次入参都是一个分组后的Seq
   * @return
   * 按分组顺序返回每个线程处理后的结果集
   */
  def parallelProcess[T, R](data: Seq[T], executor: ThreadPoolExecutor)(fun: Seq[T] => R): Seq[R] = {
    this.parallelProcess(data, executor.getMaximumPoolSize, executor)(fun)
  }

  /**
   * 对给定的数据集按指定线程数自动分组，并根据给定的处理逻辑（fun）进行并发处理
   * 该方法允许手动传入线程池，以提高线程复用率
   *
   * 调用示例请参考：
   * ThreadUtilsTest.testParallelProcessWithExecutor
   *
   * @param data
   * 待处理的数据集
   * @param threadNum
   * 并发分组数，实际分组数不会超过数据集大小
   * @param executor
   * 指定的线程池，用于复用线程资源
   * @param fun
   * 对每个分组数据的处理逻辑。每次入参都是一个分组后的Seq
   * @return
   * 按分组顺序返回每个线程处理后的结果集
   */
  def parallelProcess[T, R](data: Seq[T], threadNum: Int, executor: Executor)(fun: Seq[T] => R): Seq[R] = {
    if (data == null || data.isEmpty) return Seq.empty[R]

    val parallelism = math.min(math.abs(threadNum), data.size)
    val groupSize = math.ceil(data.size.toDouble / parallelism).toInt
    val groupedData = data.grouped(groupSize).toSeq
    this.parallelProcessEach(groupedData, executor)(group => fun(group))
  }

  /**
   * 对给定的数据集逐条进行多线程并发处理（每次调用会创建线程池）
   * 与 `parallelProcess` 不同的是，该方法不会先对数据集分组，而是直接将每条数据
   * 作为一个独立任务提交到线程池中执行，并发度由 `threadNum` 参数决定
   *
   * 调用示例请参考：
   * ThreadUtilsTest.testParallelProcessEachWithThreadNum
   *
   * @param data
   * 待处理的数据集
   * @param threadNum
   * 并发线程数
   * @param fun
   * 对每条数据的处理逻辑。每次入参都是单条数据
   * @return
   * 按原始数据顺序返回每条数据处理后的结果集
   */
  def parallelProcessEach[T, R](data: Seq[T], threadNum: Int)(fun: T => R): Seq[R] = {
    if (data == null || data.isEmpty) return Seq.empty[R]
    require(threadNum != 0, s"线程数不能为0，当前值：$threadNum")

    val executor = this.createThreadPool(ThreadPoolType.FIXED, math.abs(threadNum))
    try {
      this.parallelProcessEach(data, executor)(fun)
    } finally {
      this.shutdown(executor)
    }
  }

  /**
   * 对给定的数据集逐条进行多线程并发处理，并通过指定线程池执行
   * 该方法不会先对数据集分组，而是直接将每条数据作为一个独立任务提交到线程池中执行
   *
   * 调用示例请参考：
   * ThreadUtilsTest.testParallelProcessEachWithExecutor
   *
   * @param data
   * 待处理的数据集
   * @param executor
   * 指定的线程池，用于复用线程资源
   * @param fun
   * 对每条数据的处理逻辑。每次入参都是单条数据
   * @return
   * 按原始数据顺序返回每条数据处理后的结果集
   */
  def parallelProcessEach[T, R](data: Seq[T], executor: Executor)(fun: T => R): Seq[R] = {
    if (data == null || data.isEmpty) return Seq.empty[R]

    val asyncFutures = data.map(item => {
      CompletableFuture.supplyAsync(new Supplier[R] {
        override def get(): R = fun(item)
      }, executor)
    })

    CompletableFuture.allOf(asyncFutures: _*).join()
    asyncFutures.map(_.join())
  }

  /**
   * 对给定的数据集逐条进行多线程并发处理，并通过指定 `ThreadPoolExecutor` 执行
   * 该方法不会先对数据集分组，而是直接将每条数据作为一个独立任务提交到线程池中执行
   *
   * 调用示例请参考：
   * ThreadUtilsTest.testParallelProcessEachWithThreadPoolExecutor
   *
   * @param data
   * 待处理的数据集
   * @param executor
   * 指定的 `ThreadPoolExecutor`，用于复用线程资源
   * @param fun
   * 对每条数据的处理逻辑。每次入参都是单条数据
   * @return
   * 按原始数据顺序返回每条数据处理后的结果集
   */
  def parallelProcessEach[T, R](data: Seq[T], executor: ThreadPoolExecutor)(fun: T => R): Seq[R] = {
    this.parallelProcessEach(data, executor.asInstanceOf[Executor])(fun)
  }
}
