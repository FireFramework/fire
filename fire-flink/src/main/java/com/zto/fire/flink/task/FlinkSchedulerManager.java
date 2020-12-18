package com.zto.fire.flink.task;

import com.zto.fire.core.task.SchedulerManager;

/**
 * Flink 定时调度任务管理器
 *
 * @author ChengLong
 * @create 2020-12-18 17:20
 * @since 1.0.0
 */
public class FlinkSchedulerManager extends SchedulerManager {
    // 单例对象
    private static SchedulerManager instance = null;

    static {
        instance = new FlinkSchedulerManager();
    }

    private FlinkSchedulerManager() {
    }

    /**
     * 获取单例实例
     */
    public static SchedulerManager getInstance() {
        return instance;
    }

    @Override
    protected String label() {
        return DRIVER;
    }
}
