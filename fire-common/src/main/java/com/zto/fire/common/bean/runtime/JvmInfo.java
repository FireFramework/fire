package com.zto.fire.common.bean.runtime;

import java.lang.management.*;
import java.util.List;

/**
 * Jvm信息包装类，可获取jvm相关信息
 * @author ChengLong 2019-9-28 19:38:36
 */
public class JvmInfo {
    // Java版本
    private String javaVersion;
    // JavaHome
    private String javaHome;
    // 类版本
    private String classVersion;
    // jvm可从操作系统申请的最大内存
    private long memoryMax;
    // jvm已使用操作系统的总内存空间
    private long memoryTotal;
    // jvm剩余内存空间
    private long memoryFree;
    // jvm已使用内存空间
    private long memoryUsed;
    // jvm启动时间，unix时间戳
    private long startTime;
    // jvm运行时间
    private long uptime;
    // jvm heap 初始内存大小
    private long heapInitSize;
    // jvm heap 最大内存空间
    private long heapMaxSize;
    // jvm heap 已使用空间大小
    private long heapUseSize;
    // jvm heap 已提交的空间大小
    private long heapCommitedSize;
    // jvm Non-Heap初始空间
    private long nonHeapInitSize;
    // jvm Non-Heap最大空间
    private long nonHeapMaxSize;
    // jvm Non-Heap已使用空间
    private long nonHeapUseSize;
    // jvm Non-Heap已提交空间
    private long nonHeapCommitedSize;
    // gc 总数
    private long gcCount;
    // gc 时长
    private long gcTime;

    private JvmInfo() {}

    public long getMemoryMax() {
        return memoryMax;
    }

    public long getMemoryTotal() {
        return memoryTotal;
    }

    public long getMemoryFree() {
        return memoryFree;
    }

    public long getMemoryUsed() {
        return memoryUsed;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getUptime() {
        return uptime;
    }

    public long getHeapInitSize() {
        return heapInitSize;
    }

    public long getHeapMaxSize() {
        return heapMaxSize;
    }

    public long getHeapUseSize() {
        return heapUseSize;
    }

    public long getHeapCommitedSize() {
        return heapCommitedSize;
    }

    public long getNonHeapInitSize() {
        return nonHeapInitSize;
    }

    public long getNonHeapMaxSize() {
        return nonHeapMaxSize;
    }

    public long getNonHeapUseSize() {
        return nonHeapUseSize;
    }

    public long getNonHeapCommitedSize() {
        return nonHeapCommitedSize;
    }

    public String getJavaVersion() {
        return javaVersion;
    }

    public String getJavaHome() {
        return javaHome;
    }

    public String getClassVersion() {
        return classVersion;
    }

    public long getGcCount() {
        return gcCount;
    }

    public long getGcTime() {
        return gcTime;
    }

    /**
     * 获取Jvm、类加载器与线程相关信息
     */
    public static JvmInfo getJvmInfo() {
        Runtime runtime = Runtime.getRuntime();
        JvmInfo jvmInfo = new JvmInfo();
        jvmInfo.memoryMax = runtime.maxMemory();
        jvmInfo.memoryTotal = runtime.totalMemory();
        jvmInfo.memoryFree = runtime.freeMemory();
        jvmInfo.memoryUsed = jvmInfo.memoryTotal - jvmInfo.memoryFree;
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        jvmInfo.startTime = runtimeMXBean.getStartTime();
        jvmInfo.uptime = runtimeMXBean.getUptime();

        // 获取jvm heap相关信息
        MemoryMXBean memoryMBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heapUsage = memoryMBean.getHeapMemoryUsage();
        jvmInfo.heapInitSize = heapUsage.getInit();
        jvmInfo.heapMaxSize = heapUsage.getMax();
        jvmInfo.heapUseSize = heapUsage.getUsed();
        jvmInfo.heapCommitedSize = heapUsage.getCommitted();

        // 获取jvm non-heap相关信息
        MemoryUsage nonHeapUsage = memoryMBean.getNonHeapMemoryUsage();
        jvmInfo.nonHeapInitSize = nonHeapUsage.getInit();
        jvmInfo.nonHeapMaxSize = nonHeapUsage.getMax();
        jvmInfo.nonHeapUseSize = nonHeapUsage.getUsed();
        jvmInfo.nonHeapCommitedSize = nonHeapUsage.getCommitted();

        // 获取jvm版本与安装信息
        jvmInfo.javaVersion = System.getProperty("java.version");
        jvmInfo.javaHome = System.getProperty("java.home");
        jvmInfo.classVersion = System.getProperty("java.class.version");

        // 获取gc信息
        List<GarbageCollectorMXBean> gcs = ManagementFactory.getGarbageCollectorMXBeans();
        for (GarbageCollectorMXBean gc : gcs) {
            jvmInfo.gcCount = gc.getCollectionCount();
            jvmInfo.gcTime = gc.getCollectionTime();
        }

        return jvmInfo;
    }
}
