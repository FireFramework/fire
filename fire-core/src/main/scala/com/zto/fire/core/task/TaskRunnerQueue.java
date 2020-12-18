package com.zto.fire.core.task;

import org.quartz.DisallowConcurrentExecution;

/**
 * 线程安全的方式执行定时任务，同一实例同一时刻只能有一个任务
 * @author ChengLong 2019年11月5日 09:59:33
 * @since 0.3.5
 */
@DisallowConcurrentExecution
public class TaskRunnerQueue extends TaskRunner {
}
