package com.zto.fire.common.enu;

/**
 * Fire任务类型
 *
 * @author ChengLong 2019-7-26 11:06:38
 */
public enum JobType {
    SPARK_CORE("spark_core"), SPARK_STREAMING("spark_streaming"), SPARK_STRUCTURED_STREAMING("spark_structured_streaming"), SPARK_SQL("spark_sql"), FLINK_STREAMING("flink_streaming"), FLINK_BATCH("flink_batch"), UNDEFINED("undefined");

    // 任务类型
    private final String jobType;

    JobType(String jobType) {
        this.jobType = jobType;
    }

    /**
     * 获取当前任务的类型
     *
     * @return
     */
    public String getJobType() {
        return this.jobType;
    }

    /**
     * 用于判断当前任务是否为spark任务
     *
     * @return true: spark任务  false：非spark任务
     */
    public boolean isSpark() {
        if (this.jobType.contains("spark")) {
            return true;
        }
        return false;
    }

    /**
     * 用于判断当前任务是否为flink任务
     *
     * @return true: flink任务  false：非flink任务
     */
    public boolean isFlink() {
        if (this.jobType.contains("flink")) {
            return true;
        }
        return false;
    }
}
