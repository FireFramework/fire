package com.zto.bigdata.spark

import java.util.concurrent.Executors

object Test {
  val threadPool = Executors.newFixedThreadPool(20)

  def main(args: Array[String]): Unit = {
    (1 to 1).foreach(i => {
      runAsThreadLoop(printThread, 1000)
    })
    println("=======main======" + Thread.currentThread().getName)
    threadPool.shutdown()
  }

  def printThread(): Unit = {
    println("======sub=======" + Thread.currentThread().getName)
  }

  def runAsThread(fun: => Unit): Unit = {
    this.threadPool.execute(new Runnable {
      override def run(): Unit = {
        fun
      }
    })
  }

  def runAsThreadLoop(fun: => Unit, delay: Long): Unit = {
    this.threadPool.execute(new Runnable {
      override def run(): Unit = {
        while (true) {
          fun
          Thread.sleep(delay)
        }
      }
    })
  }
}
