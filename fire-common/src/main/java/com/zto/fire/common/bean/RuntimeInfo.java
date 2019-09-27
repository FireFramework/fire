package com.zto.fire.common.bean;

import com.alibaba.fastjson.JSON;
import com.sun.management.OperatingSystemMXBean;
import com.sun.management.ThreadMXBean;
import com.zto.fire.common.util.SystemInfoUtils;

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;

public class RuntimeInfo implements Serializable {
    // Java版本
    private String javaVersion = System.getProperty("java.version");
    // JavaHome
    private String javaHome = System.getProperty("java.home");
    // 类版本
    private String classVersion = System.getProperty("java.class.version");
    // 操作系统名称
    private String osName = System.getProperty("os.name");
    // 操作系统架构
    private String osArch = System.getProperty("os.arch");
    // 操作系统版本
    private String osVersion = System.getProperty("os.version");
    // 当前用户
    private String userName = System.getProperty("user.name");
    // 当前用户家目录
    private String userHome = System.getProperty("user.home");
    // 当前用户工作目录
    private String userDir = System.getProperty("user.dir");
    // 机器的ip
    private String ip = SystemInfoUtils.getIp();
    // 集群的主机名
    private String hostname = SystemInfoUtils.getHostName();
    // jvm可从操作系统申请的最大内存
    private long jvmMemoryMax;
    // jvm已使用操作系统的总内存空间
    private long jvmMemoryTotal;
    // jvm剩余内存空间
    private long jvmMemoryFree;
    // jvm已使用内存空间
    private long jvmMemoryUse;
    // jvm启动时间，unix时间戳
    private long jvmStartTime;
    // jvm运行时间
    private long jvmUptime;
    // jvm heap 初始内存大小
    private long jvmHeapInitSize;
    // jvm heap 最大内存空间
    private long jvmHeapMaxSize;
    // jvm heap 已使用空间大小
    private long jvmHeapUseSize;
    // jvm heap 已提交的空间大小
    private long jvmHeapCommitedSize;
    // jvm Non-Heap初始空间
    private long jvmNonHeapInitSize;
    // jvm Non-Heap最大空间
    private long jvmNonHeapMaxSize;
    // jvm Non-Heap已使用空间
    private long jvmNonHeapUseSize;
    // jvm Non-Heap已提交空间
    private long jvmNonHeapCommitedSize;
    // 操作系统总内存空间
    private long osMemoryTotal;
    // 操作系统内存剩余空间
    private long osMemoryFree;
    // 操作系统内存使用空间
    private long osMemoryUse;
    // 操作系统提交的虚拟内存大小
    private long osMemoryCommitVirtualSize;
    // 操作系统交换内存总空间
    private long osSwapMemoryTotal;
    // 操作系统交换内存剩余空间
    private long osSwapMemoryFree;
    // 操作系统交换内存已使用空间
    private long osSwapMemoryUse;
    // 系统最近1分钟的负载
    private double systemLoadAverage;
    // 系统cpu的负载
    private double systemCpuLoad;
    // 当前jvm可用的处理器数量
    private int availableProcessors;
    // 当前jvm占用的cpu时长
    private long processCpuTime;
    // 当前jvm占用的cpu负载
    private double processCpuLoad;
    // 当前线程的总 CPU 时间（以毫微秒为单位）
    private long currentThreadCpuTime;
    // 当前线程的总用户cpu时间（以毫微秒为单位）
    private long currentThreadUserTime;
    // 当前守护线程的总数
    private int currentThreadDeamonCount;
    // 返回自从 Java 虚拟机启动或峰值重置以来峰值活动线程计数
    private int peakThreadCount;
    // 返回当前线程的总数，包括守护线程和非守护线程
    private int threadCount;
    // 返回自从 Java 虚拟机启动以来创建和启动的线程总数目
    private long totalStartedThreadCount;

    public String getJavaVersion() {
        return javaVersion;
    }

    public void setJavaVersion(String javaVersion) {
        this.javaVersion = javaVersion;
    }

    public String getJavaHome() {
        return javaHome;
    }

    public void setJavaHome(String javaHome) {
        this.javaHome = javaHome;
    }

    public String getClassVersion() {
        return classVersion;
    }

    public void setClassVersion(String classVersion) {
        this.classVersion = classVersion;
    }

    public String getOsName() {
        return osName;
    }

    public void setOsName(String osName) {
        this.osName = osName;
    }

    public String getOsArch() {
        return osArch;
    }

    public void setOsArch(String osArch) {
        this.osArch = osArch;
    }

    public String getOsVersion() {
        return osVersion;
    }

    public void setOsVersion(String osVersion) {
        this.osVersion = osVersion;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getUserHome() {
        return userHome;
    }

    public void setUserHome(String userHome) {
        this.userHome = userHome;
    }

    public String getUserDir() {
        return userDir;
    }

    public void setUserDir(String userDir) {
        this.userDir = userDir;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public long getJvmMemoryMax() {
        return jvmMemoryMax;
    }

    public void setJvmMemoryMax(long jvmMemoryMax) {
        this.jvmMemoryMax = jvmMemoryMax;
    }

    public long getJvmMemoryTotal() {
        return jvmMemoryTotal;
    }

    public void setJvmMemoryTotal(long jvmMemoryTotal) {
        this.jvmMemoryTotal = jvmMemoryTotal;
    }

    public long getJvmMemoryFree() {
        return jvmMemoryFree;
    }

    public void setJvmMemoryFree(long jvmMemoryFree) {
        this.jvmMemoryFree = jvmMemoryFree;
    }

    public long getJvmMemoryUse() {
        return jvmMemoryUse;
    }

    public void setJvmMemoryUse(long jvmMemoryUse) {
        this.jvmMemoryUse = jvmMemoryUse;
    }

    public long getJvmStartTime() {
        return jvmStartTime;
    }

    public void setJvmStartTime(long jvmStartTime) {
        this.jvmStartTime = jvmStartTime;
    }

    public long getJvmUptime() {
        return jvmUptime;
    }

    public void setJvmUptime(long jvmUptime) {
        this.jvmUptime = jvmUptime;
    }

    public long getJvmHeapInitSize() {
        return jvmHeapInitSize;
    }

    public void setJvmHeapInitSize(long jvmHeapInitSize) {
        this.jvmHeapInitSize = jvmHeapInitSize;
    }

    public long getJvmHeapMaxSize() {
        return jvmHeapMaxSize;
    }

    public void setJvmHeapMaxSize(long jvmHeapMaxSize) {
        this.jvmHeapMaxSize = jvmHeapMaxSize;
    }

    public long getJvmHeapUseSize() {
        return jvmHeapUseSize;
    }

    public void setJvmHeapUseSize(long jvmHeapUseSize) {
        this.jvmHeapUseSize = jvmHeapUseSize;
    }

    public long getJvmHeapCommitedSize() {
        return jvmHeapCommitedSize;
    }

    public void setJvmHeapCommitedSize(long jvmHeapCommitedSize) {
        this.jvmHeapCommitedSize = jvmHeapCommitedSize;
    }

    public long getJvmNonHeapInitSize() {
        return jvmNonHeapInitSize;
    }

    public void setJvmNonHeapInitSize(long jvmNonHeapInitSize) {
        this.jvmNonHeapInitSize = jvmNonHeapInitSize;
    }

    public long getJvmNonHeapMaxSize() {
        return jvmNonHeapMaxSize;
    }

    public void setJvmNonHeapMaxSize(long jvmNonHeapMaxSize) {
        this.jvmNonHeapMaxSize = jvmNonHeapMaxSize;
    }

    public long getJvmNonHeapUseSize() {
        return jvmNonHeapUseSize;
    }

    public void setJvmNonHeapUseSize(long jvmNonHeapUseSize) {
        this.jvmNonHeapUseSize = jvmNonHeapUseSize;
    }

    public long getJvmNonHeapCommitedSize() {
        return jvmNonHeapCommitedSize;
    }

    public void setJvmNonHeapCommitedSize(long jvmNonHeapCommitedSize) {
        this.jvmNonHeapCommitedSize = jvmNonHeapCommitedSize;
    }

    public long getOsMemoryTotal() {
        return osMemoryTotal;
    }

    public void setOsMemoryTotal(long osMemoryTotal) {
        this.osMemoryTotal = osMemoryTotal;
    }

    public long getOsMemoryFree() {
        return osMemoryFree;
    }

    public void setOsMemoryFree(long osMemoryFree) {
        this.osMemoryFree = osMemoryFree;
    }

    public long getOsMemoryUse() {
        return osMemoryUse;
    }

    public void setOsMemoryUse(long osMemoryUse) {
        this.osMemoryUse = osMemoryUse;
    }

    public long getOsMemoryCommitVirtualSize() {
        return osMemoryCommitVirtualSize;
    }

    public void setOsMemoryCommitVirtualSize(long osMemoryCommitVirtualSize) {
        this.osMemoryCommitVirtualSize = osMemoryCommitVirtualSize;
    }

    public long getOsSwapMemoryTotal() {
        return osSwapMemoryTotal;
    }

    public void setOsSwapMemoryTotal(long osSwapMemoryTotal) {
        this.osSwapMemoryTotal = osSwapMemoryTotal;
    }

    public long getOsSwapMemoryFree() {
        return osSwapMemoryFree;
    }

    public void setOsSwapMemoryFree(long osSwapMemoryFree) {
        this.osSwapMemoryFree = osSwapMemoryFree;
    }

    public long getOsSwapMemoryUse() {
        return osSwapMemoryUse;
    }

    public void setOsSwapMemoryUse(long osSwapMemoryUse) {
        this.osSwapMemoryUse = osSwapMemoryUse;
    }

    public double getSystemLoadAverage() {
        return systemLoadAverage;
    }

    public void setSystemLoadAverage(double systemLoadAverage) {
        this.systemLoadAverage = systemLoadAverage;
    }

    public double getSystemCpuLoad() {
        return systemCpuLoad;
    }

    public void setSystemCpuLoad(double systemCpuLoad) {
        this.systemCpuLoad = systemCpuLoad;
    }

    public int getAvailableProcessors() {
        return availableProcessors;
    }

    public void setAvailableProcessors(int availableProcessors) {
        this.availableProcessors = availableProcessors;
    }

    public long getProcessCpuTime() {
        return processCpuTime;
    }

    public void setProcessCpuTime(long processCpuTime) {
        this.processCpuTime = processCpuTime;
    }

    public double getProcessCpuLoad() {
        return processCpuLoad;
    }

    public void setProcessCpuLoad(double processCpuLoad) {
        this.processCpuLoad = processCpuLoad;
    }

    public long getCurrentThreadCpuTime() {
        return currentThreadCpuTime;
    }

    public void setCurrentThreadCpuTime(long currentThreadCpuTime) {
        this.currentThreadCpuTime = currentThreadCpuTime;
    }

    public long getCurrentThreadUserTime() {
        return currentThreadUserTime;
    }

    public void setCurrentThreadUserTime(long currentThreadUserTime) {
        this.currentThreadUserTime = currentThreadUserTime;
    }

    public int getCurrentThreadDeamonCount() {
        return currentThreadDeamonCount;
    }

    public void setCurrentThreadDeamonCount(int currentThreadDeamonCount) {
        this.currentThreadDeamonCount = currentThreadDeamonCount;
    }

    public int getPeakThreadCount() {
        return peakThreadCount;
    }

    public void setPeakThreadCount(int peakThreadCount) {
        this.peakThreadCount = peakThreadCount;
    }

    public int getThreadCount() {
        return threadCount;
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }

    public long getTotalStartedThreadCount() {
        return totalStartedThreadCount;
    }

    public void setTotalStartedThreadCount(long totalStartedThreadCount) {
        this.totalStartedThreadCount = totalStartedThreadCount;
    }

    private RuntimeInfo() {
    }

    /**
     * 获取运行时信息
     *
     * @return 当前运行时信息
     */
    public static RuntimeInfo getRuntimeInfo() {
        RuntimeInfo runtimeInfo = new RuntimeInfo();
        // 获取jvm相关信息
        Runtime runtime = Runtime.getRuntime();
        runtimeInfo.jvmMemoryMax = runtime.maxMemory();
        runtimeInfo.jvmMemoryTotal = runtime.totalMemory();
        runtimeInfo.jvmMemoryFree = runtime.freeMemory();
        runtimeInfo.jvmMemoryUse = runtimeInfo.jvmMemoryTotal - runtimeInfo.jvmMemoryFree;
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        runtimeInfo.jvmStartTime = runtimeMXBean.getStartTime();
        runtimeInfo.jvmUptime = runtimeMXBean.getUptime();
        // 获取jvm heap相关信息
        MemoryMXBean memoryMBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heapUsage = memoryMBean.getHeapMemoryUsage();
        runtimeInfo.jvmHeapInitSize = heapUsage.getInit();
        runtimeInfo.jvmHeapMaxSize = heapUsage.getMax();
        runtimeInfo.jvmHeapUseSize = heapUsage.getUsed();
        runtimeInfo.jvmHeapCommitedSize = heapUsage.getCommitted();

        // 获取jvm non-heap相关信息
        MemoryUsage nonHeapUsage = memoryMBean.getNonHeapMemoryUsage();
        runtimeInfo.jvmNonHeapInitSize = nonHeapUsage.getInit();
        runtimeInfo.jvmNonHeapMaxSize = nonHeapUsage.getMax();
        runtimeInfo.jvmNonHeapUseSize = nonHeapUsage.getUsed();
        runtimeInfo.jvmNonHeapCommitedSize = nonHeapUsage.getCommitted();

        // 获取操作系统内存相关信息
        OperatingSystemMXBean osmxb = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        runtimeInfo.osMemoryTotal = osmxb.getTotalPhysicalMemorySize();
        runtimeInfo.osMemoryFree = osmxb.getFreePhysicalMemorySize();
        runtimeInfo.osMemoryUse = runtimeInfo.osMemoryTotal - runtimeInfo.osMemoryFree;
        runtimeInfo.osSwapMemoryTotal = osmxb.getTotalSwapSpaceSize();
        runtimeInfo.osSwapMemoryFree = osmxb.getFreeSwapSpaceSize();
        runtimeInfo.osSwapMemoryUse = runtimeInfo.osSwapMemoryTotal - runtimeInfo.osSwapMemoryFree;
        runtimeInfo.osMemoryCommitVirtualSize = osmxb.getCommittedVirtualMemorySize();
        runtimeInfo.systemLoadAverage = osmxb.getSystemLoadAverage();
        runtimeInfo.systemCpuLoad = osmxb.getSystemCpuLoad();
        runtimeInfo.availableProcessors = osmxb.getAvailableProcessors();
        runtimeInfo.processCpuTime = osmxb.getProcessCpuTime();
        runtimeInfo.processCpuLoad = osmxb.getProcessCpuLoad();

        // 获取线程相关信息
        ThreadMXBean threadMBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        runtimeInfo.currentThreadCpuTime = threadMBean.getCurrentThreadCpuTime();
        runtimeInfo.currentThreadUserTime = threadMBean.getCurrentThreadUserTime();
        runtimeInfo.currentThreadDeamonCount = threadMBean.getDaemonThreadCount();
        runtimeInfo.peakThreadCount = threadMBean.getPeakThreadCount();
        runtimeInfo.threadCount = threadMBean.getThreadCount();
        runtimeInfo.totalStartedThreadCount = threadMBean.getTotalStartedThreadCount();

        return runtimeInfo;
    }

    public static void main(String[] args) {
        for (int i = 0; i < 10; i++) {
            System.out.println(JSON.toJSONString(RuntimeInfo.getRuntimeInfo()));
        }
    }
}
