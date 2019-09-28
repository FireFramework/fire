package com.zto.fire.common.bean.runtime;

import com.sun.management.OperatingSystemMXBean;

import java.lang.management.ManagementFactory;

/**
 * 用于封装cpu运行时信息
 *
 * @author ChengLong 2019-9-28 19:52:56
 */
public class CpuInfo {
    // 系统最近1分钟的负载
    private double loadAverage;
    // 系统cpu的负载
    private double cpuLoad;
    // 当前jvm可用的处理器数量
    private int availableProcessors;
    // 当前jvm占用的cpu时长
    private long processCpuTime;
    // 当前jvm占用的cpu负载
    private double processCpuLoad;

    public double getLoadAverage() {
        return loadAverage;
    }

    public double getCpuLoad() {
        return cpuLoad;
    }

    public int getAvailableProcessors() {
        return availableProcessors;
    }

    public long getProcessCpuTime() {
        return processCpuTime;
    }

    public double getProcessCpuLoad() {
        return processCpuLoad;
    }

    private CpuInfo() {
    }

    /**
     * 获取cpu使用信息
     */
    public static CpuInfo getCpuInfo() {
        CpuInfo cpuInfo = new CpuInfo();
        OperatingSystemMXBean osmxb = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        cpuInfo.loadAverage = osmxb.getSystemLoadAverage();
        cpuInfo.cpuLoad = osmxb.getSystemCpuLoad();
        cpuInfo.availableProcessors = osmxb.getAvailableProcessors();
        cpuInfo.processCpuTime = osmxb.getProcessCpuTime();
        cpuInfo.processCpuLoad = osmxb.getProcessCpuLoad();

        return cpuInfo;
    }
}
