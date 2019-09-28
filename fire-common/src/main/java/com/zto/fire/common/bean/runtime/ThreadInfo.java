package com.zto.fire.common.bean.runtime;

import com.sun.management.ThreadMXBean;

import java.lang.management.ManagementFactory;

/**
 * 用于包装运行时线程信息
 * @author ChengLong 2019-9-28 19:36:52
 */
public class ThreadInfo {
    // 当前线程的总 CPU 时间（以毫微秒为单位）
    private long cpuTime;
    // 当前线程的总用户cpu时间（以毫微秒为单位）
    private long userTime;
    // 当前守护线程的总数
    private int deamonCount;
    // 返回自从 Java 虚拟机启动或峰值重置以来峰值活动线程计数
    private int peakCount;
    // 返回当前线程的总数，包括守护线程和非守护线程
    private int totalCount;
    // 返回自从 Java 虚拟机启动以来创建和启动的线程总数目
    private long totalStartedCount;

    private ThreadInfo() {}

    public long getCpuTime() {
        return cpuTime;
    }

    public long getUserTime() {
        return userTime;
    }

    public int getDeamonCount() {
        return deamonCount;
    }

    public int getPeakCount() {
        return peakCount;
    }

    public int getTotalCount() {
        return totalCount;
    }

    public long getTotalStartedCount() {
        return totalStartedCount;
    }

    /**
     * 获取线程相关信息
     */
    public static ThreadInfo getThreadInfo() {
        ThreadInfo threadInfo = new ThreadInfo();
        ThreadMXBean threadMBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        threadInfo.cpuTime = threadMBean.getCurrentThreadCpuTime();
        threadInfo.userTime = threadMBean.getCurrentThreadUserTime();
        threadInfo.deamonCount = threadMBean.getDaemonThreadCount();
        threadInfo.peakCount = threadMBean.getPeakThreadCount();
        threadInfo.totalCount = threadMBean.getThreadCount();
        threadInfo.totalStartedCount = threadMBean.getTotalStartedThreadCount();

        return threadInfo;
    }
}
