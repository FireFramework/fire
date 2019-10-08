package com.zto.fire.common.bean.runtime;

import com.alibaba.fastjson.JSON;

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

    /**
     * 获取运行时信息
     *
     * @return 当前运行时信息
     */
    public static RuntimeInfo getRuntimeInfo() {
        runtimeInfo.jvmInfo = JvmInfo.getJvmInfo();
        runtimeInfo.classLoaderInfo = ClassLoaderInfo.getClassLoaderInfo();
        runtimeInfo.threadInfo = ThreadInfo.getThreadInfo();
        runtimeInfo.osInfo = OSInfo.getOSInfo();
        runtimeInfo.cpuInfo = CpuInfo.getCpuInfo();
        runtimeInfo.memoryInfo = MemoryInfo.getMemoryInfo();
        runtimeInfo.diskInfo = DiskInfo.getDiskInfo();
        runtimeInfo.hardwareInfo = HardwareInfo.getHardwareInfo();

        return runtimeInfo;
    }

    public static void main(String[] args) throws Exception {
        for (int i = 0; i < 10; i++) {
            System.out.println(JSON.toJSONString(RuntimeInfo.getRuntimeInfo()) + "\n");
            Thread.sleep(1000);
        }
    }
}
