package com.zto.fire.spark.task;

import com.zto.fire.core.task.SchedulerManager;
import org.apache.spark.SparkEnv;

/**
 * Spark 定时调度任务管理器
 *
 * @author ChengLong
 * @create 2020-12-18 17:00
 * @since 1.0.0
 */
public class SparkSchedulerManager extends SchedulerManager {
    // 单例对象
    private static SchedulerManager instance = null;

    static {
        instance = new SparkSchedulerManager();
    }

    private SparkSchedulerManager() {}

    /**
     * 获取单例实例
     */
    public static SchedulerManager getInstance() {
        return instance;
    }

    @Override
    protected String label() {
        SparkEnv sparkEnv = SparkEnv.get();
        if (sparkEnv == null || DRIVER.equalsIgnoreCase(sparkEnv.executorId())) {
            return DRIVER;
        } else {
            return EXECUTOR;
        }
    }
}
