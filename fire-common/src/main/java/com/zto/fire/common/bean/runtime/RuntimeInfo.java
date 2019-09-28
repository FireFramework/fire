package com.zto.fire.common.bean.runtime;

import com.alibaba.fastjson.JSON;

import java.io.Serializable;

/**
 * 用于获取jvm、os、memory等运行时信息，获取速度较慢，比较重
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

    private RuntimeInfo() {}

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

        return runtimeInfo;
    }

    public static void main(String[] args) throws Exception {
        for (int i = 0; i < 10; i++) {
            System.out.println(JSON.toJSONString(RuntimeInfo.getRuntimeInfo()));
        }
        System.out.println("===============================");
        Thread.sleep(60000);
        long start = System.currentTimeMillis();
        System.out.println(JSON.toJSONString(RuntimeInfo.getRuntimeInfo()) + "\n 耗时：" + (System.currentTimeMillis() - start));

    }
}
