package com.zto.fire.common.bean;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;
import com.zto.fire.common.util.DateFormatUtils;
import com.zto.fire.common.util.SystemInfoUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.TaskMetrics;

import java.io.Serializable;

/**
 * 用于记录任务的执行时间
 *
 * @author ChengLong 2019-6-10 16:16:16
 */
public class TimeCost implements Serializable {
    private static Boolean isExecutor;
    // 起始时间
    private String startTime;
    private String endTime;
    @JSONField(serialize = false)
    private Long start;
    // 目标源
    private String sink;
    // 异常信息
    private String msg;
    // 执行的操作
    private String action;
    // 耗时
    private Long timeCost;
    // 用于区分埋点日志和用户日志
    private Boolean sdk = false;
    // 处理结果（true：成功 false：失败）
    private Boolean result;
    private TaskContext taskInfo;
    private String hostname;
    private String ip;
    private String load;
    private Integer stageId;
    private Long taskId;
    private Integer partitionId;
    private Long executorCpuTime;
    private Long executorRunTime;
    private Long jvmGCTime;
    private Long executorDeserializeCpuTime;
    private Long executorDeserializeTime;
    private Long bytesRead;
    private Long recordsRead;
    private Long memoryBytesSpilled;
    private Long bytesWritten;
    private Long recordsWritten;
    private Long peakExecutionMemory;
    private Long resultSize;

    public String getLoad() {
        return load;
    }

    public String getEndTime() {
        return endTime;
    }

    public Boolean getResult() {
        return result;
    }

    public String getStartTime() {
        return startTime;
    }

    public String getSink() {
        return sink;
    }

    public String getMsg() {
        return msg;
    }

    public String getAction() {
        return action;
    }

    public Long getTimeCost() {
        if (this.timeCost == null) {
            return System.currentTimeMillis() - this.start;
        }
        return timeCost;
    }

    public String getHostname() {
        return hostname;
    }

    public String getIp() {
        return ip;
    }

    public TaskContext getTaskInfo() {
        return taskInfo;
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

    public Long getExecutorCpuTime() {
        return executorCpuTime;
    }

    public Long getExecutorRunTime() {
        return executorRunTime;
    }

    public Long getJvmGCTime() {
        return jvmGCTime;
    }

    public Long getExecutorDeserializeCpuTime() {
        return executorDeserializeCpuTime;
    }

    public Long getExecutorDeserializeTime() {
        return executorDeserializeTime;
    }

    public Long getBytesRead() {
        return bytesRead;
    }

    public Long getRecordsRead() {
        return recordsRead;
    }

    public Long getMemoryBytesSpilled() {
        return memoryBytesSpilled;
    }

    public Long getBytesWritten() {
        return bytesWritten;
    }

    public Long getRecordsWritten() {
        return recordsWritten;
    }

    public Long getPeakExecutionMemory() {
        return peakExecutionMemory;
    }

    public Long getResultSize() {
        return resultSize;
    }

    public Boolean getSdk() {
        return sdk;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }

    private TimeCost() {
        this.start = System.currentTimeMillis();
        this.startTime = DateFormatUtils.formatCurrentDateTime();
        this.ip = SystemInfoUtils.getIp();
        this.hostname = SystemInfoUtils.getHostName();
        this.load = SystemInfoUtils.getLoadAverage();
        isExecutor = !SparkEnv.get().executorId().equalsIgnoreCase("driver");
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
     * @param sink   目标源：hbase、oracle、mysql等
     * @param action 执行的动作：insert、delete、update、select
     * @return 当前对象
     */
    public TimeCost info(String sink, String action, String msg, Boolean sdk) {
        this.sink = sink;
        this.action = action;
        if (sdk != null) this.sdk = sdk;
        if (isExecutor) {
            this.taskInfo = TaskContext.get();
            if (this.taskInfo != null) {
                this.taskId = this.taskInfo.taskAttemptId();
                this.stageId = this.taskInfo.stageId();
                this.partitionId = this.taskInfo.partitionId();
                TaskMetrics taskMetrics = this.taskInfo.taskMetrics();
                if (taskMetrics != null) {
                    this.executorCpuTime = taskMetrics.executorCpuTime();
                    this.executorRunTime = taskMetrics.executorRunTime();
                    this.jvmGCTime = taskMetrics.jvmGCTime();
                    this.executorDeserializeCpuTime = taskMetrics.executorDeserializeCpuTime();
                    this.executorDeserializeTime = taskMetrics.executorDeserializeTime();
                    this.bytesRead = taskMetrics.inputMetrics().bytesRead();
                    this.recordsRead = taskMetrics.inputMetrics().recordsRead();
                    this.memoryBytesSpilled = taskMetrics.memoryBytesSpilled();
                    this.bytesWritten = taskMetrics.outputMetrics().bytesWritten();
                    this.recordsWritten = taskMetrics.outputMetrics().recordsWritten();
                    this.peakExecutionMemory = taskMetrics.peakExecutionMemory();
                    this.resultSize = taskMetrics.resultSize();
                }
            }
        }
        this.timeCost = System.currentTimeMillis() - this.start;
        this.endTime = DateFormatUtils.formatCurrentDateTime();
        if (StringUtils.isBlank(msg)) {
            this.msg = "success";
            this.result = true;
        } else {
            this.msg = msg;
            this.result = false;
        }
        return this;
    }
}