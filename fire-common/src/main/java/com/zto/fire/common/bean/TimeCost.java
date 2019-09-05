package com.zto.fire.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import com.zto.fire.common.util.DateFormatUtils;
import com.zto.fire.common.util.StackTraceUtils;
import com.zto.fire.common.util.SystemInfoUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;

import java.io.Serializable;
import java.util.UUID;

/**
 * 用于记录任务的执行时间
 *
 * @author ChengLong 2019-6-10 16:16:16
 */
public class TimeCost implements Serializable {
    // 异常信息
    private String msg;
    // 耗时
    private Long timeCost;
    private String ip;
    private String load;

    // 用于区分埋点日志和用户日志
    private Boolean isFire = false;
    private String id = UUID.randomUUID().toString();
    // 任务的applicationId
    private static String applicationId;
    // 任务的main方法
    private static String mainClass;
    // executorId
    private static String executorId;
    private Integer stageId;
    private Long taskId;
    private Integer partitionId;
    @JSONField(serialize = false)
    private Throwable exception;
    private String stackTraceInfo;
    private String level = "WARN";
    private String peripheral;
    private Integer io;
    private Long start;
    private String startTime;
    private String endTime;

    public String getId() {
        return id;
    }

    public String getLoad() {
        return load;
    }

    public String getMsg() {
        return msg;
    }

    public Long getTimeCost() {
        if (this.timeCost == null) {
            return System.currentTimeMillis() - this.start;
        }
        return timeCost;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public String getIp() {
        return ip;
    }

    public Integer getStageId() {
        return stageId;
    }

    public Long getTaskId() {
        return taskId;
    }

    public Integer getPartitionId() {
        return partitionId;
    }

    public Boolean getIsFire() {
        return isFire;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    public String getExecutorId() {
        return executorId;
    }

    public String getMainClass() {
        return mainClass;
    }

    public void setMainClass(String mainClass) {
        this.mainClass = mainClass;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public void setTimeCost(Long timeCost) {
        this.timeCost = timeCost;
    }

    public Boolean getFire() {
        return isFire;
    }

    public void setFire(Boolean fire) {
        isFire = fire;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public void setLoad(String load) {
        this.load = load;
    }

    public void setStageId(Integer stageId) {
        this.stageId = stageId;
    }

    public void setTaskId(Long taskId) {
        this.taskId = taskId;
    }

    public void setPartitionId(Integer partitionId) {
        this.partitionId = partitionId;
    }

    public Long getStart() {
        return start;
    }

    public void setStart(Long start) {
        this.start = start;
    }

    public String getStackTraceInfo() {
        return stackTraceInfo;
    }

    public void setStackTraceInfo(String stackTraceInfo) {
        this.stackTraceInfo = stackTraceInfo;
    }

    public String getPeripheral() {
        return peripheral;
    }

    public Integer getIo() {
        return io;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    private String lable() {
        if (this.isFire) {
            return "fire";
        } else {
            return "user";
        }
    }

    @Override
    public String toString() {
        String baseInfo = "【" + this.lable() + "Log】 " + this.msg + " start：" + this.startTime + " end：" + this.endTime + " cost：" + this.timeCost + " ip：" + this.ip + " load：" + this.load + " executor：" + this.executorId;
        if (!"driver".equalsIgnoreCase(this.executorId)) {
            baseInfo += " stage：" + this.stageId + " task：" + this.taskId;
        }
        if (this.isFire) {
            baseInfo += " peripheral：" + this.peripheral + " io：" + this.io;
        }
        return baseInfo;
    }

    private TimeCost() {
        this.start = System.currentTimeMillis();
        this.startTime = DateFormatUtils.formatCurrentDateTime();
        this.ip = SystemInfoUtils.getIp();
        this.load = SystemInfoUtils.getLoadAverageCache();

        if (SparkEnv.get() != null) {
            if (StringUtils.isBlank(executorId)) {
                executorId = SparkEnv.get().executorId();
            }
            if (StringUtils.isBlank(applicationId)) {
                applicationId = SparkEnv.get().conf().get("spark.app.id", "");
            }
            if (StringUtils.isBlank(mainClass)) {
                mainClass = SparkEnv.get().conf().get("spark.driver.class.name", "");
            }
        }
    }

    /**
     * 构建一个TimCost对象
     *
     * @return 返回TimeCost对象实例
     */
    public static TimeCost build() {
        return new TimeCost();
    }

    /**
     * 设置必要的参数
     *
     * @return 当前对象
     */
    public TimeCost info(String msg, String peripheral, Integer io, Boolean isFire, Throwable exception) {
        this.timeCost = System.currentTimeMillis() - this.start;
        this.endTime = DateFormatUtils.formatCurrentDateTime();
        this.exception = exception;
        this.msg = msg;
        this.peripheral = peripheral;
        this.io = io;
        if (isFire != null) this.isFire = isFire;
        if (StringUtils.isNotBlank(this.executorId) && !"driver".equalsIgnoreCase(this.executorId)) {
            TaskContext taskInfo = TaskContext.get();
            if (taskInfo != null) {
                this.taskId = taskInfo.taskAttemptId();
                this.stageId = taskInfo.stageId();
                this.partitionId = taskInfo.partitionId();
            }
        }
        if (exception != null) {
            this.stackTraceInfo = StackTraceUtils.stackTraceInfo(exception);
            this.level = "ERROR";
        }
        return this;
    }
}