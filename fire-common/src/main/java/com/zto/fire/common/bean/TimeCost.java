package com.zto.fire.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import com.zto.fire.common.conf.FireConf;
import com.zto.fire.common.util.DateFormatUtils;
import com.zto.fire.common.util.StackTraceUtils;
import com.zto.fire.common.util.SystemInfoUtils;
import com.zto.fire.common.util.TimeCostUtils;
import org.apache.commons.lang3.StringUtils;
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
    // 多核cpu使用率
    private String cpuUsage;
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
    private String module;
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

    public static String getApplicationId() {
        return applicationId;
    }

    public static void setApplicationId(String applicationId) {
        TimeCost.applicationId = applicationId;
    }

    public static String getExecutorId() {
        return executorId;
    }

    public static String getMainClass() {
        return mainClass;
    }

    public static void setExecutorId(String executorId) {
        TimeCost.executorId = executorId;
    }

    public static void setMainClass(String mainClass) {
        TimeCost.mainClass = mainClass;
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

    public String getModule() {
        return module;
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

    public String getCpuUsage() {
        return cpuUsage;
    }

    public void setCpuUsage(String cpuUsage) {
        this.cpuUsage = cpuUsage;
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
        String baseInfo = "【" + this.lable() + "Log】 〖" + this.msg + "〗    start：" + this.startTime + " end：" + this.endTime + " cost：" + this.getTimeCost() + " ip：" + this.ip + " load：" + this.load + " cpuUsage：" + this.cpuUsage + " executor：" + this.executorId;
        if (!"driver".equalsIgnoreCase(this.executorId)) {
            baseInfo += " stage：" + this.stageId + " task：" + this.taskId;
        }
        if (this.isFire) {
            baseInfo += " module：" + this.module + " io：" + this.io;
        }
        return baseInfo;
    }

    private TimeCost() {
        this.start = System.currentTimeMillis();
        this.startTime = DateFormatUtils.formatCurrentDateTime();
        this.ip = SystemInfoUtils.getIp();
        this.load = SystemInfoUtils.getLoadAverageCache();
        this.cpuUsage = SystemInfoUtils.getCpuUsageCache();
        if (FireConf.isSparkEngine()) {
            // 如果是spark引擎，则获取spark相关运行时信息
            TimeCostUtils.getEngineInfo();
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
    public TimeCost info(String msg, String module, Integer io, Boolean isFire, Throwable exception) {
        this.timeCost = System.currentTimeMillis() - this.start;
        this.endTime = DateFormatUtils.formatCurrentDateTime();
        this.exception = exception;
        this.msg = msg;
        this.module = module;
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