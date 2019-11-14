package com.zto.fire.common.bean.runtime;

import com.alibaba.fastjson.JSON;
import com.zto.fire.common.util.SystemInfoUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkEnv;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * 用于获取jvm、os、memory等运行时信息
 *
 * @author ChengLong 2019年9月28日 16:57:03
 */
public class RuntimeInfo implements Serializable {
    private static RuntimeInfo runtimeInfo = new RuntimeInfo();
    // jvm运行时信息
    private JvmInfo jvmInfo;
    // 操作系统信息
    private OSInfo osInfo;
    // 线程运行时信息
    private ThreadInfo threadInfo;
    // cpu运行时信息
    private CpuInfo cpuInfo;
    // 内存运行时信息
    private MemoryInfo memoryInfo;
    // 类加载器运行时信息
    private ClassLoaderInfo classLoaderInfo;
    // 磁盘及分区信息
    private Map<String, List> diskInfo;
    // 设备信息
    private HardwareInfo hardwareInfo;
    // executorId号或driver
    private static String executorId;
    // executor所在ip
    private static String ip;
    // executor所在主机名
    private static String hostname;
    // 当前pid的进程号
    private static String pid;
    // executor启动时间（UNIX时间戳）
    private long startTime = System.currentTimeMillis();
    // executor运行时间（毫秒）
    private long uptime;

    private RuntimeInfo() {
    }

    public JvmInfo getJvmInfo() {
        return jvmInfo;
    }

    public OSInfo getOsInfo() {
        return osInfo;
    }

    public ThreadInfo getThreadInfo() {
        return threadInfo;
    }

    public CpuInfo getCpuInfo() {
        return cpuInfo;
    }

    public MemoryInfo getMemoryInfo() {
        return memoryInfo;
    }

    public ClassLoaderInfo getClassLoaderInfo() {
        return classLoaderInfo;
    }

    public Map<String, List> getDiskInfo() {
        return diskInfo;
    }

    public HardwareInfo getHardwareInfo() {
        return hardwareInfo;
    }

    public String getExecutorId() {
        return executorId;
    }

    public String getIp() {
        return ip;
    }

    public String getHostname() {
        return hostname;
    }

    public String getPid() {
        return pid;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getUptime() {
        this.uptime = System.currentTimeMillis() - this.startTime;
        return uptime;
    }

    /**
     * 获取运行时信息
     *
     * @return 当前运行时信息
     */
    public static RuntimeInfo getRuntimeInfo() {
        SparkEnv sparkEnv = SparkEnv.get();
        if (sparkEnv != null) {
            if (StringUtils.isBlank(executorId)) {
                executorId = sparkEnv.executorId();
            }
        }
        if (StringUtils.isBlank(ip)) {
            ip = SystemInfoUtils.getIp();
        }
        if (StringUtils.isBlank(hostname)) {
            hostname = SystemInfoUtils.getHostName();
        }
        if (StringUtils.isBlank(pid)) {
            pid = SystemInfoUtils.getPid();
        }
        runtimeInfo.jvmInfo = JvmInfo.getJvmInfo();
        runtimeInfo.classLoaderInfo = ClassLoaderInfo.getClassLoaderInfo();
        runtimeInfo.threadInfo = ThreadInfo.getThreadInfo();
        runtimeInfo.cpuInfo = CpuInfo.getCpuInfo();
        runtimeInfo.memoryInfo = MemoryInfo.getMemoryInfo();
        // runtimeInfo.osInfo = OSInfo.getOSInfo();
        // runtimeInfo.diskInfo = DiskInfo.getDiskInfo();
        // runtimeInfo.hardwareInfo = HardwareInfo.getHardwareInfo();

        return runtimeInfo;
    }

    public static void main(String[] args) throws Exception {
        for (int i = 0; i < 10; i++) {
            System.out.println(JSON.toJSONString(RuntimeInfo.getRuntimeInfo()) + "\n");
            Thread.sleep(1000);
        }
    }
}
