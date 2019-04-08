package com.zto.bigdata.spark.common.util;

import com.zto.bigdata.spark.common.bean.SystemLoadInfo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

/**
 * 用于获取服务器负载信息，包括磁盘io、cpu负载、内存使用、网络使用等等
 *
 * @author ChengLong 2019-04-08 13:57:31
 */
public class SystemInfoUtils {
    private static final float totalBandwidth = 80; // 设定带宽，Mbps
    private static SystemLoadInfo systemLoadInfo;

    private SystemInfoUtils() {
        this.systemLoadInfo = new SystemLoadInfo();
    }

    /**
     * 采集CPU使用率（兼容多核）
     *
     * @return
     */
    public static SystemLoadInfo getCpuInfo() {
        float cpuUsage = 0;
        Process pro1 = null;
        Process pro2 = null;
        Runtime runtime = Runtime.getRuntime();
        BufferedReader in1 = null;
        BufferedReader in2 = null;
        try {
            String command = "cat /proc/stat";

            //第一次采集CPU时间
            pro1 = runtime.exec(command);
            in1 = new BufferedReader(new InputStreamReader(pro1.getInputStream()));
            String line = null;
            long idleCpuTime1 = 0;
            long totalCpuTime1 = 0;    //分别为系统启动后空闲的CPU时间和总的CPU时间
            while ((line = in1.readLine()) != null) {
                if (line.startsWith("cpu")) {
                    line = line.trim();
                    String[] temp = line.split("\\s+");
                    idleCpuTime1 = Long.parseLong(temp[4]);
                    for (String s : temp) {
                        if (!s.equals("cpu")) {
                            totalCpuTime1 += Long.parseLong(s);
                        }
                    }
                    break;
                }
            }

            //第二次采集CPU时间
            Thread.sleep(100);
            pro2 = runtime.exec(command);
            in2 = new BufferedReader(new InputStreamReader(pro2.getInputStream()));
            long idleCpuTime2 = 0;
            long totalCpuTime2 = 0;    //分别为系统启动后空闲的CPU时间和总的CPU时间
            while ((line = in2.readLine()) != null) {
                if (line.startsWith("cpu")) {
                    line = line.trim();
                    String[] temp = line.split("\\s+");
                    idleCpuTime2 = Long.parseLong(temp[4]);
                    for (String s : temp) {
                        if (!s.equals("cpu")) {
                            totalCpuTime2 += Long.parseLong(s);
                        }
                    }
                    break;
                }
            }
            if (idleCpuTime1 != 0 && totalCpuTime1 != 0 && idleCpuTime2 != 0 && totalCpuTime2 != 0) {
                cpuUsage = 1 - (float) (idleCpuTime2 - idleCpuTime1) / (float) (totalCpuTime2 - totalCpuTime1);
                systemLoadInfo.setCpuUsage(cpuUsage);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.close(in1, in2);
            IOUtils.close(pro1, pro2);
        }
        return systemLoadInfo;
    }

    /**
     * 采集内存及Swap的数据封装入bean属性
     *
     * @return bean
     */
    public static SystemLoadInfo getMemoryInfo() {
        float memUsage = 0.0f;
        float swapUsage = 0.0f;
        Process pro = null;
        Runtime runtime = Runtime.getRuntime();
        BufferedReader in = null;
        try {
            String command = "cat /proc/meminfo";
            pro = runtime.exec(command);
            in = new BufferedReader(new InputStreamReader(pro.getInputStream()));
            String line = null;

            long totalMem = 0;
            long freeMem = 0;
            long totalSwap = 0;
            long freeSwap = 0;
            while ((line = in.readLine()) != null) {
                String[] memInfo = line.split("\\s+");
                if (memInfo[0].startsWith("MemTotal")) {
                    totalMem = Long.parseLong(memInfo[1]);
                }
                if (memInfo[0].startsWith("MemFree")) {
                    freeMem = Long.parseLong(memInfo[1]);
                }
                memUsage = 1 - (float) freeMem / (float) totalMem;
                if (memInfo[0].startsWith("SwapTotal")) {
                    totalSwap = Long.parseLong(memInfo[1]);
                }
                if (memInfo[0].startsWith("SwapFree")) {
                    freeSwap = Long.parseLong(memInfo[1]);
                }
                swapUsage = 1 - (float) freeSwap / (float) totalSwap;
                if (totalMem != 0 && totalSwap != 0 && freeMem != 0 && freeSwap != 0) {
                    systemLoadInfo.setMemoryTotal(totalMem);
                    systemLoadInfo.setMemoryfree(freeMem);
                    systemLoadInfo.setMemoryUsage(memUsage);
                    systemLoadInfo.setSwapTotal(totalSwap);
                    systemLoadInfo.setSwapfree(freeSwap);
                    systemLoadInfo.setSwapUsage(swapUsage);
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.close(in);
            IOUtils.close(pro);
        }
        return systemLoadInfo;
    }

    /**
     * 采集磁盘IO使用率，在统计时间内所有处理IO时间，除以总共统计时间。例如，如果统计间隔1秒，该设备有0.8
     * 秒在处理IO，而0.2秒闲置，那么该设备的%util = 0.8/1 = 80%，所以该参数暗示了设备的繁忙程度
     *
     * @return bean
     */
    public static SystemLoadInfo getIOInfo() {
        float ioUsage = 0.0f;
        Process pro = null;
        BufferedReader in = null;
        Runtime r = Runtime.getRuntime();
        try {
            String command = "iostat -d -x";
            pro = r.exec(command);
            in = new BufferedReader(new InputStreamReader(pro.getInputStream()));
            String line = null;
            int count = 0;
            List<String> infoIO = new ArrayList<>();
            List<SystemLoadInfo.DiskInfo> infoDiskNode = new ArrayList<>();
            while ((line = in.readLine()) != null) {
                if (count == 0) {
                    String[] ioInfo = line.split("\\s+");
                    systemLoadInfo.setLinuxCoreVersion(ioInfo[0] + " " + ioInfo[1]);
                    String temp2 = ioInfo[2];
                    systemLoadInfo.setHostNameFromIOCommand(temp2.substring(1, temp2.length() - 1));
                    systemLoadInfo.setSystemTimeInChinese(ioInfo[3]);
                    systemLoadInfo.setSystemType(ioInfo[4].substring(1, ioInfo[4].length() - 1));
                    systemLoadInfo.setCoreNum(Byte.valueOf(ioInfo[5].substring(1, 2)));
                }

                if (count++ >= 3) {
                    String[] temp = line.split("\\s+");
                    if (temp.length > 1) {
                        float util = Float.parseFloat(temp[temp.length - 1]);
                        ioUsage = (ioUsage > util) ? ioUsage : util;
                        SystemLoadInfo.DiskInfo diskNode = new SystemLoadInfo.DiskInfo();
                        diskNode.setDevice(temp[0]);
                        diskNode.setRrqmS(Float.valueOf(temp[1]));
                        diskNode.setWrqmS(Float.valueOf(temp[2]));
                        diskNode.setrS(Float.valueOf(temp[3]));
                        diskNode.setwS(Float.valueOf(temp[4]));
                        diskNode.setRkBs(Float.valueOf(temp[5]));
                        diskNode.setWkBs(Float.valueOf(temp[6]));
                        diskNode.setAvgrqSZ(Float.valueOf(temp[7]));
                        diskNode.setAvgquSZ(Float.valueOf(temp[8]));
                        diskNode.setAwait(Float.valueOf(temp[9]));
                        diskNode.setrAWAIT(Float.valueOf(temp[10]));
                        diskNode.setwAWAIT(Float.valueOf(temp[11]));
                        diskNode.setSvctm(Float.valueOf(temp[12]));
                        diskNode.setUtil(Float.valueOf(temp[13]));
                        infoDiskNode.add(diskNode);
                    }
                }
                infoIO.add(line);
            }
            if (ioUsage > 0) {
                systemLoadInfo.setDiskNode(infoDiskNode);
                systemLoadInfo.setiOUsage(ioUsage);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.close(in);
            IOUtils.close(pro);
        }
        return systemLoadInfo;
    }

    /**
     * 将"cat /proc/net/dev"中所有信息封装入bean参数
     *
     * @return bean
     */
    public static SystemLoadInfo getNetInfo() {
        float netUsage = 0.0f;
        Process pro1 = null;
        Process pro2 = null;
        Runtime r = Runtime.getRuntime();
        BufferedReader in1 = null;
        BufferedReader in2 = null;
        try {
            String command = "cat /proc/net/dev";
            //第一次采集流量数据
            long startTime = System.currentTimeMillis();
            pro1 = r.exec(command);
            in1 = new BufferedReader(new InputStreamReader(pro1.getInputStream()));
            String line = null;
            long inSize1 = 0;
            long outSize1 = 0;
            int count = 0;
            while ((line = in1.readLine()) != null) {
                line = line.trim();
                if (++count >= 3) { // 自己监控的网卡
                    String[] temp = line.split("\\s+");
                    //截取掉网卡名
                    inSize1 += Long.parseLong(temp[1]);    //Receive bytes,单位为Byte
                    outSize1 += Long.parseLong(temp[9]);   //Transmit bytes,单位为Byte
                }
            }
            //第二次采集流量数据，采集网卡数据
            Thread.sleep(1000);
            long endTime = System.currentTimeMillis();
            pro2 = r.exec(command);
            in2 = new BufferedReader(new InputStreamReader(pro2.getInputStream()));
            long inSize2 = 0;
            long outSize2 = 0;
            count = 0;
            long recieveBytes = 0;
            long recievePackets = 0;
            long recieveErrs = 0;
            long recieveDrop = 0;
            long recieveFifo = 0;
            long recieveFrame = 0;
            long recieveCompressed = 0;
            long recieveMulticast = 0;
            long transmitBytes = 0;
            long transmitPackets = 0;
            long transmitErrs = 0;
            long transmitDrop = 0;
            long transmitFifo = 0;
            long transmitColls = 0;
            long transmitCarrier = 0;
            long transmitCompressed = 0;
            while ((line = in2.readLine()) != null) {
                line = line.trim();
                if (++count >= 3) { // 这里选择监控的网卡
                    String[] temp = line.split("\\s+");
                    inSize2 += Long.parseLong(temp[1]);
                    outSize2 += Long.parseLong(temp[9]);
                    recieveBytes += Long.parseLong(temp[1]);
                    recievePackets += Long.parseLong(temp[2]);
                    recieveErrs += Long.parseLong(temp[3]);
                    recieveDrop += Long.parseLong(temp[4]);
                    recieveFifo += Long.parseLong(temp[5]);
                    recieveFrame += Long.parseLong(temp[6]);
                    recieveCompressed += Long.parseLong(temp[7]);
                    recieveMulticast += Long.parseLong(temp[8]);
                    transmitBytes += Long.parseLong(temp[9]);
                    transmitPackets += Long.parseLong(temp[10]);
                    transmitErrs += Long.parseLong(temp[11]);
                    transmitDrop += Long.parseLong(temp[12]);
                    transmitFifo += Long.parseLong(temp[13]);
                    transmitColls += Long.parseLong(temp[14]);
                    transmitCarrier += Long.parseLong(temp[15]);
                    transmitCompressed += Long.parseLong(temp[16]);
                }
            }

            if (inSize1 != 0 && outSize1 != 0 && inSize2 != 0 && outSize2 != 0) {
                systemLoadInfo.setRecieveBytes(recieveBytes);
                systemLoadInfo.setRecievePackets(recievePackets);
                systemLoadInfo.setRecieveErrs(recieveErrs);
                systemLoadInfo.setRecieveDrop(recieveDrop);
                systemLoadInfo.setRecieveFifo(recieveFifo);
                systemLoadInfo.setRecieveFrame(recieveFrame);
                systemLoadInfo.setRecieveCompressed(recieveCompressed);
                systemLoadInfo.setRecieveMulticast(recieveMulticast);
                systemLoadInfo.setTransmitBytes(transmitBytes);
                systemLoadInfo.setTransmitPackets(transmitPackets);
                systemLoadInfo.setTransmitErrs(transmitErrs);
                systemLoadInfo.setTransmitDrop(transmitDrop);
                systemLoadInfo.setTransmitFifo(transmitFifo);
                systemLoadInfo.setTransmitColls(transmitColls);
                systemLoadInfo.setTransmitCarrier(transmitCarrier);
                systemLoadInfo.setTransmitCompressed(transmitCompressed);
                float interval = (float) (endTime - startTime) / 1000;
                //网口传输速度单位为Mbps
                float curRate = (float) (inSize2 - inSize1 + outSize2 - outSize1) * 8 / (1000000 * interval);
                netUsage = curRate / totalBandwidth;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.close(in1, in2);
            IOUtils.close(pro1, pro2);
        }
        return systemLoadInfo;
    }

    /**
     * 获取uptime命令及"cat /proc/loadavg"的数据，封装入bean属性
     *
     * @return bean
     */
    public static SystemLoadInfo getUpTime() {
        String upTimeInfo = ""; // 控制台输出信息
        Process pro = null;
        Process pro2 = null;
        BufferedReader in = null;
        BufferedReader in2 = null;
        Runtime r = Runtime.getRuntime();
        try {
            String command = "uptime";
            pro = r.exec(command);
            in = new BufferedReader(new InputStreamReader(pro.getInputStream()));
            String line = null;
            while ((line = in.readLine()) != null) {
                upTimeInfo = line;
            }

            if (upTimeInfo != null) {
                String[] upTime = upTimeInfo.split(",\\s+");
                String[] upTimeTemp = upTime[0].split("up\\s+");
                int length = upTime.length;
                systemLoadInfo.setCurrentSystemTime(upTimeTemp[0]);
                systemLoadInfo.setLivedTime(upTimeTemp[1]);

                //这个坑：有的时候，uptime输出的语句，会出现四个参数，而大多数的时候，都是三个。
                if (upTime[length - 4] != null && upTime[length - 4].endsWith("users")) {
                    systemLoadInfo.setUserNum(Integer.valueOf(upTime[length - 4].split("\\s+")[0]));
                }

                if (upTime[length - 3] != null && upTime[length - 3].startsWith("load")) {
                    systemLoadInfo.setLoadAverage(upTime[length - 3] + "," + upTime[length - 2] + "," + upTime[length - 1]);
                }
            }

            // 开始收集memory使用率
            Runtime r2 = Runtime.getRuntime();
            command = "cat /proc/loadavg";
            pro2 = r2.exec(command);
            in2 = new BufferedReader(new InputStreamReader(pro2.getInputStream()));
            line = null;
            while ((line = in2.readLine()) != null) {
                String[] loadAvgInfo = line.split("\\s+");
                systemLoadInfo.setLoadAverage1(Float.valueOf(loadAvgInfo[0]));
                systemLoadInfo.setLoadAverage5(Float.valueOf(loadAvgInfo[1]));
                systemLoadInfo.setLoadAverage15(Float.valueOf(loadAvgInfo[2]));
                systemLoadInfo.setProccessStatus(loadAvgInfo[3]);
                systemLoadInfo.setRecentlyProccessID(Integer.valueOf(loadAvgInfo[4]));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.close(in, in2);
            IOUtils.close(pro, pro2);
        }
        return systemLoadInfo;
    }

    /**
     * 获取主机地址信息
     *
     * @return
     */
    public static InetAddress getHostLANAddress() {
        try {
            InetAddress candidateAddress = null;
            // 遍历所有的网络接口
            for (Enumeration ifaces = NetworkInterface.getNetworkInterfaces(); ifaces.hasMoreElements(); ) {
                NetworkInterface iface = (NetworkInterface) ifaces.nextElement();
                // 在所有的接口下再遍历IP
                for (Enumeration inetAddrs = iface.getInetAddresses(); inetAddrs.hasMoreElements(); ) {
                    InetAddress inetAddr = (InetAddress) inetAddrs.nextElement();
                    if (!inetAddr.isLoopbackAddress()) {
                        // 排除loopback类型地址
                        if (inetAddr.isSiteLocalAddress()) {
                            // 如果是site-local地址
                            return inetAddr;
                        } else if (candidateAddress == null) {
                            // site-local类型的地址未被发现，先记录候选地址
                            candidateAddress = inetAddr;
                        }
                    }
                }
            }
            if (candidateAddress != null) {
                return candidateAddress;
            }
            // 如果没有发现 non-loopback地址.只能用最次选的方案
            InetAddress jdkSuppliedAddress = InetAddress.getLocalHost();
            return jdkSuppliedAddress;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 获取本机的ip地址
     *
     * @return ip地址
     */
    public static String getIp() {
        InetAddress inetAddress = getHostLANAddress();
        if (inetAddress != null) {
            return inetAddress.getHostAddress();
        }
        return "";
    }

    /**
     * 获取本机的hostname
     *
     * @return hostname
     */
    public static String getHostName() {
        InetAddress inetAddress = getHostLANAddress();
        if (inetAddress != null) {
            return inetAddress.getHostName();
        }
        return "";
    }

    /**
     * 获取ip地址
     *
     * @return
     */
    public static SystemLoadInfo getIP() {
        systemLoadInfo.setIp(getIp());
        systemLoadInfo.setHostName(getHostName());
        return systemLoadInfo;
    }

    /**
     * 获取系统全部的负载信息，包括cpu、内存、磁盘io、网络等
     *
     * @return
     */
    public static SystemLoadInfo getSystemLoadInfo() {
        SystemInfoUtils.getCpuInfo();
        SystemInfoUtils.getMemoryInfo();
        SystemInfoUtils.getNetInfo();
        SystemInfoUtils.getIOInfo();
        SystemInfoUtils.getUpTime();
        SystemInfoUtils.getIP();
        return SystemInfoUtils.systemLoadInfo;
    }
}
