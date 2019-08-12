package com.zto.fire.demo.jobs

import org.quartz._
import org.quartz.impl.StdSchedulerFactory

object JobTest {

  def main(args: Array[String]): Unit = {
    val job = JobBuilder.newJob(classOf[SparkJob]).build()
    val trigger = TriggerBuilder.newTrigger.withSchedule(CronScheduleBuilder.cronSchedule("0 */1 * * * ?")).build
    //3创建Scheduler(任务调度)对象//3创建Scheduler(任务调度)对象
    try {
      val scheduler = StdSchedulerFactory.getDefaultScheduler
      scheduler.scheduleJob(job, trigger)
      scheduler.start()
    } catch {
      case e: SchedulerException =>
        e.printStackTrace()
    }
  }
}
