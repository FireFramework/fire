/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zto.fire.common.util;

import com.zto.fire.common.bean.runtime.JvmInfo;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 进程内存监控类，用于监控指定进程及其子进程的内存使用情况
 *
 * @author chenglong
 * @since fire-2.5.3
 */
public class YarnContainerMemoryMonitor implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(YarnContainerMemoryMonitor.class);

    // 1MB的字节数
    private static final long MB = 1024 * 1024L;
    // 监控间隔（毫秒）
    private final long monitoringInterval;
    // 跟踪的进程信息映射
    private final Map<String, ProcessInfo> trackedProcesses;
    // 监控线程
    private final MonitoringThread monitoringThread;
    // 运行状态标志
    private final AtomicBoolean isRunning;
    // 最大物理内存限制（字节），-1表示无限制
    private final long maxPhysicalMemoryLimit;
    // 最大虚拟内存限制（字节）
    private final long maxVirtualMemoryLimit;
    // container的id标识
    private final String containerId;

    /**
     * 带监控间隔的构造函数
     * @param monitoringIntervalMs 监控间隔（毫秒）
     */
    public YarnContainerMemoryMonitor(long monitoringIntervalMs, long maxPhysicalMemoryLimit, long maxVirtualMemoryLimit) {
        this.monitoringInterval = monitoringIntervalMs;
        this.maxPhysicalMemoryLimit = maxPhysicalMemoryLimit;
        this.maxVirtualMemoryLimit = maxVirtualMemoryLimit;
        this.trackedProcesses = new HashMap<>();
        this.monitoringThread = new MonitoringThread();
        Optional<ContainerId> id = YarnUtils.getContainerId();
        this.containerId = id.isPresent() ? id.get().toString() : "";
        this.isRunning = new AtomicBoolean(false);
    }

    /**
     * 开始监控指定进程的内存使用
     * @param pid 进程ID
     */
    public void startMonitoring(String pid) {
        synchronized (trackedProcesses) {
            if (!trackedProcesses.containsKey(pid)) {
                ProcessInfo info = new ProcessInfo(pid, this.maxPhysicalMemoryLimit * MB, this.maxVirtualMemoryLimit * MB);
                trackedProcesses.put(pid, info);
                logger.info("开始监控进程: {}", pid);
            }
        }

        if (!isRunning.get()) {
            // 如果监控线程未运行，则启动
            start();
        }
    }

    /**
     * 停止监控指定进程
     * @param pid 进程ID
     */
    public void stopMonitoring(String pid) {
        synchronized (trackedProcesses) {
            trackedProcesses.remove(pid);
            // 记录停止监控日志
            logger.info("停止监控进程: {}", pid);
        }
    }

    /**
     * 启动监控线程
     */
    private void start() {
        if (isRunning.compareAndSet(false, true)) {
            // 启动监控线程
            monitoringThread.start();
        }
    }

    /**
     * 停止监控线程
     */
    private void stop() {
        if (isRunning.compareAndSet(true, false)) {
            // 中断线程
            monitoringThread.interrupt();
            try {
                // 等待线程结束
                monitoringThread.join();
            } catch (InterruptedException e) {
                // 恢复中断状态
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * 进程信息类，存储进程ID和内存限制
     */
    private static class ProcessInfo implements Serializable {
        // 进程ID
        private final String pid;
        // 物理内存限制（字节）
        private final long pmemLimit;
        // 虚拟内存限制（字节）
        private final long vmemLimit;

        public ProcessInfo(String pid, long pmemLimit, long vmemLimit) {
            this.pid = pid;
            this.pmemLimit = pmemLimit;
            this.vmemLimit = vmemLimit;
        }
    }

    /**
     * 监控线程类，定期检查进程内存使用情况
     */
    private class MonitoringThread extends Thread implements Serializable {
        public MonitoringThread() {
            // 设置线程名称
            super("Fire-Process-Memory-Monitor");
        }

        /**
         * 线程运行方法，循环执行监控任务
         */
        @Override
        public void run() {
            while (isRunning.get()) {
                try {
                    // 检查所有跟踪的进程
                    monitorProcesses();
                    // 休眠指定间隔
                    Thread.sleep(monitoringInterval);
                } catch (InterruptedException e) {
                    // 中断时退出循环
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.error("监控线程发生错误: {}", e.getMessage());
                }
            }
        }

        /**
         * 监控所有跟踪的进程
         */
        private void monitorProcesses() {
            List<String> pidsToCheck;
            synchronized (trackedProcesses) {
                // 获取当前跟踪的PID列表
                pidsToCheck = new ArrayList<>(trackedProcesses.keySet());
            }

            for (String pid : pidsToCheck) {
                try {
                    ProcessInfo info = trackedProcesses.get(pid);
                    // 如果进程信息不存在，跳过
                    if (info == null) continue;
                    // 获取进程树及其内存使用情况
                    ProcessTree processTree = buildProcessTree(pid);
                    // 检查总内存使用量是否超限
                    ProcessMemoryUsage totalUsage = processTree.totalUsage;

                    if (isOverLimit(pid, totalUsage, info)) {
                        // logger.warn("\n进程树 PID {}:", pid);
                        printProcessTree(processTree, "");
                        printJvmInfo();
                        // 处理超限情况
                        handleOverLimit(pid, totalUsage, info);
                    }
                } catch (Exception e) {
                    logger.error("监控进程 {} 时发生错误: {}", pid, e.getMessage());
                }
            }
        }

        /**
         * 检查进程内存使用是否超限
         * @param pid 进程ID
         * @param usage 当前内存使用情况
         * @param info 进程内存限制信息
         * @return 是否超限
         */
        private boolean isOverLimit(String pid, ProcessMemoryUsage usage, ProcessInfo info) {
            // 检查是否超过两倍限制（立即杀死条件）
            if (info.pmemLimit > 0 && usage.physicalMemory >= info.pmemLimit) {
                return true;
            }

            if (info.vmemLimit > 0 && usage.virtualMemory >= info.vmemLimit) {
                return true;
            }

            return false;
        }
        /**
         * 处理内存超限的进程
         * @param pid 进程ID
         * @param usage 当前内存使用情况
         * @param info 进程内存限制信息
         */
        private void handleOverLimit(String pid, ProcessMemoryUsage usage, ProcessInfo info) {
            logger.warn(
                    "Container [pid={}, Id={}] 物理内存: {}/{}, 虚拟内存: {}/{}. 可能被yarn kill",
                    pid,
                    containerId,
                    formatSize(usage.physicalMemory), formatSize(info.pmemLimit),
                    formatSize(usage.virtualMemory), formatSize(info.vmemLimit)
            );

            /*try {
                // 杀死进程树
                killProcessTree(pid);
                synchronized (trackedProcesses) {
                    // 从跟踪列表中移除
                    trackedProcesses.remove(pid);
                }
            } catch (Exception e) {
                logger.error("杀死进程 {} 失败: {}", pid, e.getMessage());
            }*/
        }
    }

    /**
     * 进程树节点类，表示进程及其子进程的结构
     */
    private static class ProcessTree implements Serializable {
        // 进程ID
        String pid;
        // 当前进程的内存使用
        ProcessMemoryUsage usage;
        // 子进程列表
        List<ProcessTree> children;
        // 包括子进程的总内存使用
        ProcessMemoryUsage totalUsage;

        ProcessTree(String pid) {
            this.pid = pid;
            this.children = new ArrayList<>();
            this.usage = new ProcessMemoryUsage(0, 0);
            this.totalUsage = new ProcessMemoryUsage(0, 0);
        }
    }

    /**
     * 构建进程树
     * @param rootPid 根进程ID
     * @return 进程树根节点
     * @throws Exception 如果获取进程信息失败
     */
    private ProcessTree buildProcessTree(String rootPid) throws Exception {
        Map<String, ProcessTree> pidToNode = new HashMap<>();
        Map<String, String> pidToParent = getProcessHierarchy();

        // 创建根节点
        ProcessTree root = new ProcessTree(rootPid);
        pidToNode.put(rootPid, root);

        // 为所有进程创建节点并获取内存使用
        for (String pid : pidToParent.keySet()) {
            if (!pidToNode.containsKey(pid)) {
                pidToNode.put(pid, new ProcessTree(pid));
            }
            pidToNode.get(pid).usage = getSingleProcessMemoryUsage(pid);
        }

        // 构建树结构
        for (Map.Entry<String, String> entry : pidToParent.entrySet()) {
            String pid = entry.getKey();
            String parentPid = entry.getValue();

            if (pidToNode.containsKey(parentPid)) {
                pidToNode.get(parentPid).children.add(pidToNode.get(pid));
            }
        }

        // 计算总内存使用
        calculateTotalUsage(root);
        return root;
    }

    /**
     * 递归计算进程树的总内存使用
     * @param node 当前节点
     */
    private void calculateTotalUsage(ProcessTree node) {
        long totalPhysical = node.usage.physicalMemory;
        long totalVirtual = node.usage.virtualMemory;

        for (ProcessTree child : node.children) {
            calculateTotalUsage(child);
            totalPhysical += child.totalUsage.physicalMemory;
            totalVirtual += child.totalUsage.virtualMemory;
        }

        node.totalUsage = new ProcessMemoryUsage(totalPhysical, totalVirtual);
    }

    /**
     * 打印进程树结构
     * @param node 当前节点
     * @param prefix 前缀，用于树形显示
     */
    private void printProcessTree(ProcessTree node, String prefix) {
        logger.warn(
                "{}Container [pid={}] 物理内存:{} 虚拟内存:{}",
                prefix,
                node.pid,
                formatSize(node.usage.physicalMemory),
                formatSize(node.usage.virtualMemory)
        );

        for (int i = 0; i < node.children.size(); i++) {
            String newPrefix = prefix + (i == node.children.size() - 1 ? "└── " : "├── ");
            printProcessTree(node.children.get(i), newPrefix);
        }
    }

    /**
     * 打印jvm 内存相关信息
     */
    private void printJvmInfo() {
        JvmInfo jvmInfo = JvmInfo.getJvmInfo();
        String heapMaxSize = UnitFormatUtils.readable(jvmInfo.getHeapMaxSize(), UnitFormatUtils.DateUnitEnum.BYTE);
        String heapUseSize = UnitFormatUtils.readable(jvmInfo.getHeapUseSize(), UnitFormatUtils.DateUnitEnum.BYTE);
        float heapUsePercent = (float) jvmInfo.getHeapUseSize() / jvmInfo.getHeapMaxSize() * 100;

        String offHeapMaxSize = UnitFormatUtils.readable(jvmInfo.getNonHeapMaxSize(), UnitFormatUtils.DateUnitEnum.BYTE);
        String offHeapUseSize = UnitFormatUtils.readable(jvmInfo.getNonHeapUseSize(), UnitFormatUtils.DateUnitEnum.BYTE);
        float offHeapUsePercent = (float) jvmInfo.getNonHeapUseSize() / jvmInfo.getNonHeapMaxSize() * 100;

        logger.warn("On_Heap     :  总内存={} 已用={} 已用(%)={}%", heapMaxSize, heapUseSize, String.format("%.2f", heapUsePercent));
        logger.warn("Off_Heap    :  总内存={} 已用={} 已用(%)={}%", offHeapMaxSize, offHeapUseSize, String.format("%.2f", offHeapUsePercent));
        logger.warn("PS_Scavenge :  count={} time={}", jvmInfo.getMinorGCCount(), UnitFormatUtils.readable(jvmInfo.getMinorGCTime(), UnitFormatUtils.TimeUnitEnum.MS));
        logger.warn("PS_MarkSweep:  count={} time={}", jvmInfo.getFullGCCount(), UnitFormatUtils.readable(jvmInfo.getFullGCTime(), UnitFormatUtils.TimeUnitEnum.MS));
    }

    /**
     * 获取进程层级关系（PID到父PID的映射）
     * @return PID到PPID的映射
     * @throws Exception 如果执行ps命令失败
     */
    private Map<String, String> getProcessHierarchy() {
        Map<String, String> pidToParent = new HashMap<>();
        Process process = null;
        try {
            process = Runtime.getRuntime().exec(new String[]{"ps", "-eo", "pid,ppid"});

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                 BufferedReader errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream()))) {

                String line;
                reader.readLine(); // 跳过表头
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.trim().split("\\s+");
                    if (parts.length >= 2) {
                        pidToParent.put(parts[0], parts[1]);
                    }
                }

                int exitCode = process.waitFor();
                if (exitCode != 0) {
                    String errorOutput = errorReader.lines().reduce("", (a, b) -> a + "\n" + b);
                    logger.error("ps 命令失败: exitCode={}, error={}", exitCode, errorOutput.trim());
                }
            }
        } catch (Exception e) {
            logger.error("获取进程层级失败: {}", e.getMessage(), e);
        } finally {
            if (process != null) {
                process.destroy();
            }
        }
        return pidToParent;
    }


    /**
     * 获取单个进程的内存使用情况
     * @param pid 进程ID
     * @return 内存使用情况对象
     * @throws Exception 如果读取/proc失败
     */
    private ProcessMemoryUsage getSingleProcessMemoryUsage(String pid) {
        Process process = null;
        try {
            process = Runtime.getRuntime().exec(new String[]{"cat", "/proc/" + pid + "/status"});
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                long vmSize = 0;
                long vmRss = 0;

                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.startsWith("VmSize:")) {
                        // 转换为字节
                        vmSize = parseMemoryLine(line) * 1024;
                    } else if (line.startsWith("VmRSS:")) {
                        // 转换为字节
                        vmRss = parseMemoryLine(line) * 1024;
                    }
                }
                return new ProcessMemoryUsage(vmRss, vmSize);
            } catch (Exception e) {
                // 如果失败，返回零值
                return new ProcessMemoryUsage(0, 0);
            }
        } catch (Exception e) {
            logger.error("获取进程 {} 内存信息失败: {}", pid, e.getMessage(), e);
            return new ProcessMemoryUsage(0, 0);
        } finally {
            if (process != null) {
                process.destroy();
            }
        }
    }


    /**
     * 杀死进程树
     * @param pid 根进程ID
     * @throws Exception 如果执行kill命令失败
     */
    private void killProcessTree(String pid) throws Exception {
        Map<String, String> pidToParent = getProcessHierarchy();
        List<String> pids = new ArrayList<>();
        pids.add(pid);

        Queue<String> toProcess = new LinkedList<>(Collections.singleton(pid));
        while (!toProcess.isEmpty()) {
            String currentPid = toProcess.poll();
            for (Map.Entry<String, String> entry : pidToParent.entrySet()) {
                if (entry.getValue().equals(currentPid)) {
                    pids.add(entry.getKey());
                    toProcess.add(entry.getKey());
                }
            }
        }

        for (String processId : pids) {
            // 使用kill -9强制终止
            Runtime.getRuntime().exec(new String[]{"kill", "-9", processId});
        }
    }

    /**
     * 解析内存行数据
     * @param line 从/proc/[pid]/status读取的行
     * @return 内存值（KB）
     */
    private long parseMemoryLine(String line) {
        String[] parts = line.split("\\s+");
        return parts.length >= 2 ? Long.parseLong(parts[1]) : 0;
    }

    /**
     * 格式化内存大小为人类可读的字符串
     * @param bytes 字节数
     * @return 格式化后的字符串（B、KB、MB、GB）
     */
    private String formatSize(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < MB) return String.format("%.2f KB", bytes / 1024.0);
        if (bytes < MB * 1024) return String.format("%.2f MB", bytes / (double)MB);
        return String.format("%.2f GB", bytes / (double)(MB * 1024));
    }

    /**
     * 进程内存使用情况类
     */
    private static class ProcessMemoryUsage implements Serializable {
        // 物理内存使用量（字节）
        final long physicalMemory;
        // 虚拟内存使用量（字节）
        final long virtualMemory;

        ProcessMemoryUsage(long physicalMemory, long virtualMemory) {
            this.physicalMemory = physicalMemory;
            this.virtualMemory = virtualMemory;
        }
    }
}