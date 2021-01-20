package com.zto.fire.core.task;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.io.Serializable;

/**
 * Scheduler TaskRunner
 * @author ChengLong 2019年11月5日 09:59:33
 * @since 0.3.5
 */
public class TaskRunner implements Job, Serializable {
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        SchedulerManager.execute(context);
    }
}
