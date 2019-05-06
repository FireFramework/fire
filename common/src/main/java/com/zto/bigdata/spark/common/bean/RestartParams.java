package com.zto.bigdata.spark.common.bean;

import java.util.Map;

/**
 * 重启streaming参数
 * {"batchDuration":10,"restartSparkContext":false,"stopGracefully": false,"sparkConf":{"spark.streaming.concurrentJobs":"2"}}
 * @author ChengLong 2019-5-5 16:57:49
 */
public class RestartParams {
    // 批次时间（秒）
    private long batchDuration;
    // 是否重启SparkContext对象
    private boolean restartSparkContext;
    // 是否等待数据全部处理完成再重启
    private boolean stopGracefully;
    // 附加的conf信息
    private Map<String, String> sparkConf;

    public long getBatchDuration() {
        return batchDuration;
    }

    public void setBatchDuration(long batchDuration) {
        this.batchDuration = batchDuration;
    }

    public boolean isRestartSparkContext() {
        return restartSparkContext;
    }

    public void setRestartSparkContext(boolean restartSparkContext) {
        this.restartSparkContext = restartSparkContext;
    }

    public Map<String, String> getSparkConf() {
        return sparkConf;
    }

    public void setSparkConf(Map<String, String> sparkConf) {
        this.sparkConf = sparkConf;
    }

    public RestartParams() {
    }

    public boolean isStopGracefully() {
        return stopGracefully;
    }

    public void setStopGracefully(boolean stopGracefully) {
        this.stopGracefully = stopGracefully;
    }

    public RestartParams(long batchDuration, boolean restartSparkContext, boolean stopGracefully, Map<String, String> sparkConf) {
        this.batchDuration = batchDuration;
        this.restartSparkContext = restartSparkContext;
        this.stopGracefully = stopGracefully;
        this.sparkConf = sparkConf;
    }
}
