package com.zto.fire.common.task;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.io.Serializable;

/**
 * 用于执行定时任务，允许同一实例并行跑
 * @author ChengLong 2019年11月5日 09:38:06
 * @since 0.3.5
 */
public class TaskRunner implements Job, Serializable {

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        SchedulerManager.execute(context);
    }
}
