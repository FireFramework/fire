package com.zto.fire.demo.jobs

import com.zto.fire.common.util.DateFormatUtils
import org.quartz.{Job, JobExecutionContext}

class SparkJob extends Job {
  override def execute(jobExecutionContext: JobExecutionContext): Unit = {
    println(DateFormatUtils.formatCurrentDateTime())
  }

  def print: Unit = {
    println("=======print======")
  }

}
