package com.zto.fire.common.bean.runtime;

import com.zto.fire.common.util.SystemInfoUtils;

/**
 * 用于封装操作系统信息
 *
 * @author ChengLong 2019-9-28 19:56:59
 */
public class OSInfo {
    private static OSInfo osInfo;
    // 操作系统名称
    private String name;
    // 操作系统架构
    private String arch;
    // 操作系统版本
    private String version;
    // 当前用户
    private String userName;
    // 当前用户家目录
    private String userHome;
    // 当前用户工作目录
    private String userDir;
    // 机器的ip
    private String ip;
    // 集群的主机名
    private String hostname;

    private OSInfo() {}

    public String getName() {
        return name;
    }

    public String getArch() {
        return arch;
    }

    public String getVersion() {
        return version;
    }

    public String getUserName() {
        return userName;
    }

    public String getUserHome() {
        return userHome;
    }

    public String getUserDir() {
        return userDir;
    }

    public String getIp() {
        return ip;
    }

    public String getHostname() {
        return hostname;
    }

    /**
     * 获取操作系统相关信息
     */
    public static OSInfo getOSInfo() {
        if (osInfo == null) {
            osInfo = new OSInfo();
            osInfo.name = System.getProperty("os.name");
            osInfo.arch = System.getProperty("os.arch");
            osInfo.version = System.getProperty("os.version");
            osInfo.userName = System.getProperty("user.name");
            osInfo.userHome = System.getProperty("user.home");
            osInfo.userDir = System.getProperty("user.dir");
            osInfo.ip = SystemInfoUtils.getIp();
            osInfo.hostname = SystemInfoUtils.getHostName();
        }
        return osInfo;
    }

}
