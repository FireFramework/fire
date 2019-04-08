/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.zto.bigdata.spark.common.bean;

import java.io.Serializable;
import java.util.List;

/**
 * 封装系统负载情况的bean
 *
 * @author ChengLong 2019年4月8日13:50:11
 */
public class SystemLoadInfo implements Serializable {
    private Long recieveBytes;          //接收的字节总数
    private Long recievePackets;        //接收的数据包总数
    private Long recieveErrs;           //接收的错误总数
    private Long recieveDrop;           //接收的丢包数
    private Long recieveFifo;           //接收的FIFO缓冲区错误数量
    private Long recieveFrame;          //接收的分组帧错误数量
    private Long recieveCompressed;     //接收的压缩数据包数量
    private Long recieveMulticast;      //接收的多播帧数
    private Long transmitBytes;        //发送的字节总数
    private Long transmitPackets;      //发送的数据包总数
    private Long transmitErrs;         //发送的错误总数
    private Long transmitDrop;         //发送的丢包数
    private Long transmitFifo;         //发送的FIFO缓冲区错误数量
    private Long transmitColls;        //发送的分组帧错误数量
    private Long transmitCarrier;      //载波损耗数量
    private Long transmitCompressed;   //发送的压缩数据包数量
    private Float cpuUsage;              //cpu使用率
    private String currentSystemTime;    //当前系统时间
    private String livedTime;            //系统运行时长
    private Integer userNum;             //用户数量
    private Float loadAverage1;          //Cpu最近一分钟负载
    private Float loadAverage5;          //CPU最近5分钟负载
    private Float loadAverage15;         //CPU最近15分钟负载
    private String proccessStatus;       //“正在运行的进程数/总进程数”
    private Integer recentlyProccessID;  //最近运行的进程的ID
    private String loadAverage;          //CPU负载常规显示
    private Float iOUsage;                //各盘符中io使用率最大的；
    private String linuxCoreVersion;      //系统版本号
    private String hostNameFromIOCommand; //当前hostname
    private String systemType;            //系统类型，64位还是32位
    private Byte coreNum;                 //cpu核心数量
    private String systemTimeInChinese;   //中文格式系统时间
    private List<DiskInfo> diskNode;      //各盘符使用情况
    //获取ip和hostname
    private String ip;                    //当前ip
    private String hostName;              //当前hostName
    //内存信息
    private Long memoryTotal;             //内存总量
    private Long memoryfree;              //内存剩余
    private Float memoryUsage;            //内存使用率
    private Long swapTotal;               //Swap总量
    private Long swapfree;                //Swap剩余
    private Float swapUsage;              //Swap使用率

    public Long getRecieveBytes() {
        return recieveBytes;
    }

    public void setRecieveBytes(Long recieveBytes) {
        this.recieveBytes = recieveBytes;
    }

    public Long getRecievePackets() {
        return recievePackets;
    }

    public void setRecievePackets(Long recievePackets) {
        this.recievePackets = recievePackets;
    }

    public Long getRecieveErrs() {
        return recieveErrs;
    }

    public void setRecieveErrs(Long recieveErrs) {
        this.recieveErrs = recieveErrs;
    }

    public Long getRecieveDrop() {
        return recieveDrop;
    }

    public void setRecieveDrop(Long recieveDrop) {
        this.recieveDrop = recieveDrop;
    }

    public Long getRecieveFifo() {
        return recieveFifo;
    }

    public void setRecieveFifo(Long recieveFifo) {
        this.recieveFifo = recieveFifo;
    }

    public Long getRecieveFrame() {
        return recieveFrame;
    }

    public void setRecieveFrame(Long recieveFrame) {
        this.recieveFrame = recieveFrame;
    }

    public Long getRecieveCompressed() {
        return recieveCompressed;
    }

    public void setRecieveCompressed(Long recieveCompressed) {
        this.recieveCompressed = recieveCompressed;
    }

    public Long getRecieveMulticast() {
        return recieveMulticast;
    }

    public void setRecieveMulticast(Long recieveMulticast) {
        this.recieveMulticast = recieveMulticast;
    }

    public Long getTransmitBytes() {
        return transmitBytes;
    }

    public void setTransmitBytes(Long transmitBytes) {
        this.transmitBytes = transmitBytes;
    }

    public Long getTransmitPackets() {
        return transmitPackets;
    }

    public void setTransmitPackets(Long transmitPackets) {
        this.transmitPackets = transmitPackets;
    }

    public Long getTransmitErrs() {
        return transmitErrs;
    }

    public void setTransmitErrs(Long transmitErrs) {
        this.transmitErrs = transmitErrs;
    }

    public Long getTransmitDrop() {
        return transmitDrop;
    }

    public void setTransmitDrop(Long transmitDrop) {
        this.transmitDrop = transmitDrop;
    }

    public Long getTransmitFifo() {
        return transmitFifo;
    }

    public void setTransmitFifo(Long transmitFifo) {
        this.transmitFifo = transmitFifo;
    }

    public Long getTransmitColls() {
        return transmitColls;
    }

    public void setTransmitColls(Long transmitColls) {
        this.transmitColls = transmitColls;
    }

    public Long getTransmitCarrier() {
        return transmitCarrier;
    }

    public void setTransmitCarrier(Long transmitCarrier) {
        this.transmitCarrier = transmitCarrier;
    }

    public Long getTransmitCompressed() {
        return transmitCompressed;
    }

    public void setTransmitCompressed(Long transmitCompressed) {
        this.transmitCompressed = transmitCompressed;
    }

    public Float getCpuUsage() {
        return cpuUsage;
    }

    public void setCpuUsage(Float cpuUsage) {
        this.cpuUsage = cpuUsage;
    }

    public String getCurrentSystemTime() {
        return currentSystemTime;
    }

    public void setCurrentSystemTime(String currentSystemTime) {
        this.currentSystemTime = currentSystemTime;
    }

    public String getLivedTime() {
        return livedTime;
    }

    public void setLivedTime(String livedTime) {
        this.livedTime = livedTime;
    }

    public Integer getUserNum() {
        return userNum;
    }

    public void setUserNum(Integer userNum) {
        this.userNum = userNum;
    }

    public Float getLoadAverage1() {
        return loadAverage1;
    }

    public void setLoadAverage1(Float loadAverage1) {
        this.loadAverage1 = loadAverage1;
    }

    public Float getLoadAverage5() {
        return loadAverage5;
    }

    public void setLoadAverage5(Float loadAverage5) {
        this.loadAverage5 = loadAverage5;
    }

    public Float getLoadAverage15() {
        return loadAverage15;
    }

    public void setLoadAverage15(Float loadAverage15) {
        this.loadAverage15 = loadAverage15;
    }

    public String getProccessStatus() {
        return proccessStatus;
    }

    public void setProccessStatus(String proccessStatus) {
        this.proccessStatus = proccessStatus;
    }

    public Integer getRecentlyProccessID() {
        return recentlyProccessID;
    }

    public void setRecentlyProccessID(Integer recentlyProccessID) {
        this.recentlyProccessID = recentlyProccessID;
    }

    public String getLoadAverage() {
        return loadAverage;
    }

    public void setLoadAverage(String loadAverage) {
        this.loadAverage = loadAverage;
    }

    public Float getiOUsage() {
        return iOUsage;
    }

    public void setiOUsage(Float iOUsage) {
        this.iOUsage = iOUsage;
    }

    public String getLinuxCoreVersion() {
        return linuxCoreVersion;
    }

    public void setLinuxCoreVersion(String linuxCoreVersion) {
        this.linuxCoreVersion = linuxCoreVersion;
    }

    public String getHostNameFromIOCommand() {
        return hostNameFromIOCommand;
    }

    public void setHostNameFromIOCommand(String hostNameFromIOCommand)
    {
        this.hostNameFromIOCommand = hostNameFromIOCommand;
    }

    public String getSystemType() {
        return systemType;
    }

    public void setSystemType(String systemType) {
        this.systemType = systemType;
    }

    public Byte getCoreNum() {
        return coreNum;
    }

    public void setCoreNum(Byte coreNum) {
        this.coreNum = coreNum;
    }

    public String getSystemTimeInChinese() {
        return systemTimeInChinese;
    }

    public void setSystemTimeInChinese(String systemTimeInChinese) {
        this.systemTimeInChinese = systemTimeInChinese;
    }

    public List<DiskInfo> getDiskNode() {
        return diskNode;
    }

    public void setDiskNode(List<DiskInfo> diskNode) {
        this.diskNode = diskNode;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public Long getMemoryTotal() {
        return memoryTotal;
    }

    public void setMemoryTotal(Long memoryTotal) {
        this.memoryTotal = memoryTotal;
    }

    public Long getMemoryfree() {
        return memoryfree;
    }

    public void setMemoryfree(Long memoryfree) {
        this.memoryfree = memoryfree;
    }

    public Float getMemoryUsage() {
        return memoryUsage;
    }

    public void setMemoryUsage(Float memoryUsage) {
        this.memoryUsage = memoryUsage;
    }

    public Long getSwapTotal() {
        return swapTotal;
    }

    public void setSwapTotal(Long swapTotal) {
        this.swapTotal = swapTotal;
    }

    public Long getSwapfree() {
        return swapfree;
    }

    public void setSwapfree(Long swapfree) {
        this.swapfree = swapfree;
    }

    public Float getSwapUsage() {
        return swapUsage;
    }

    public void setSwapUsage(Float swapUsage) {
        this.swapUsage = swapUsage;
    }

    public static class DiskInfo implements Serializable {
        private String device;
        private Float rrqmS;
        private Float wrqmS;
        private Float rS;
        private Float wS;
        private Float rkBs;
        private Float wkBs;
        private Float avgrqSZ;
        private Float avgquSZ;
        private Float await;
        private Float rAWAIT;
        private Float wAWAIT;
        private Float svctm;
        private Float util;

        public String getDevice() {
            return device;
        }

        public void setDevice(String device) {
            this.device = device;
        }

        public Float getRrqmS() {
            return rrqmS;
        }

        public void setRrqmS(Float rrqmS) {
            this.rrqmS = rrqmS;
        }

        public Float getWrqmS() {
            return wrqmS;
        }

        public void setWrqmS(Float wrqmS) {
            this.wrqmS = wrqmS;
        }

        public Float getrS() {
            return rS;
        }

        public void setrS(Float rS) {
            this.rS = rS;
        }

        public Float getwS() {
            return wS;
        }

        public void setwS(Float wS) {
            this.wS = wS;
        }

        public Float getRkBs() {
            return rkBs;
        }

        public void setRkBs(Float rkBs) {
            this.rkBs = rkBs;
        }

        public Float getWkBs() {
            return wkBs;
        }

        public void setWkBs(Float wkBs) {
            this.wkBs = wkBs;
        }

        public Float getAvgrqSZ() {
            return avgrqSZ;
        }

        public void setAvgrqSZ(Float avgrqSZ) {
            this.avgrqSZ = avgrqSZ;
        }

        public Float getAvgquSZ() {
            return avgquSZ;
        }

        public void setAvgquSZ(Float avgquSZ) {
            this.avgquSZ = avgquSZ;
        }

        public Float getAwait() {
            return await;
        }

        public void setAwait(Float await) {
            this.await = await;
        }

        public Float getrAWAIT() {
            return rAWAIT;
        }

        public void setrAWAIT(Float rAWAIT) {
            this.rAWAIT = rAWAIT;
        }

        public Float getwAWAIT() {
            return wAWAIT;
        }

        public void setwAWAIT(Float wAWAIT) {
            this.wAWAIT = wAWAIT;
        }

        public Float getSvctm() {
            return svctm;
        }

        public void setSvctm(Float svctm) {
            this.svctm = svctm;
        }

        public Float getUtil() {
            return util;
        }

        public void setUtil(Float util) {
            this.util = util;
        }
    }
}
