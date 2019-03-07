package com.zto.bigdata.spark.common.bean;


import com.zto.bigdata.spark.common.anno.FieldName;

/**
 * Spark运行时日志记录表
 * Created by ChengLong on 2017-06-23.
 */
public class SparkRuntimeLog extends HBaseBaseBean<SparkRuntimeLog> {
    /**
     * 运行AppName
     */
    @FieldName("appName")
    private String appName;

    /**
     * 操作描述
     */
    @FieldName("event")
    private String event;

    /**
     * 时间戳
     */
    @FieldName("timestamp")
    private String timestamp;

    /**
     * 未处理消息记录
     */
    @FieldName("message")
    private String message;

    /**
     * 日志级别
     */
    @FieldName("logLevel")
    private String logLevel;

    /**
     * 日志详情
     */
    @FieldName("log")
    private String log;

    /**
     * 备注信息
     */
    @FieldName("remark")
    private String remark;

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getLog() {
        return log;
    }

    public void setLog(String log) {
        this.log = log;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public String getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(String logLevel) {
        this.logLevel = logLevel;
    }

    public SparkRuntimeLog() {
    }

    public SparkRuntimeLog(String appName, String message, String log, String timestamp) {
        this.appName = appName;
        this.timestamp = timestamp;
        this.message = message;
        this.log = log;
    }

    public SparkRuntimeLog(String appName, String event, String timestamp, String message, String logLevel, String log, String remark) {
        this.appName = appName;
        this.event = event;
        this.timestamp = timestamp;
        this.message = message;
        this.logLevel = logLevel;
        this.log = log;
        this.remark = remark;
    }

    @Override
    public SparkRuntimeLog buildRowKey() {
        this.rowKey = this.appName + this.timestamp;
        return this;
    }
}
