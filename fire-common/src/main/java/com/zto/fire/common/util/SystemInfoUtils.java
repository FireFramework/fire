package com.zto.fire.common.util;

import org.apache.commons.lang3.StringUtils;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.util.Enumeration;
import java.util.Random;

/**
 * 用于获取服务器负载信息，包括磁盘io、cpu负载、内存使用、网络使用等等
 * 注：使用此工具需预先安装：sudo yum install sysstat
 *
 * @author ChengLong 2019-04-08 13:57:31
 */
public class SystemInfoUtils {
    private static String ip;
    private static String hostname;
    private static String pid;
    private static final String OSNAME = "os.name";

    private SystemInfoUtils() {}

    /**
     * 获取主机地址信息
     */
    public static InetAddress getHostLANAddress() {
        try {
            InetAddress candidateAddress = null;
            // 遍历所有的网络接口
            for (Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces(); ifaces.hasMoreElements(); ) {
                NetworkInterface iface =  ifaces.nextElement();
                // 在所有的接口下再遍历IP
                for (Enumeration<InetAddress> inetAddrs = iface.getInetAddresses(); inetAddrs.hasMoreElements(); ) {
                    InetAddress inetAddr = inetAddrs.nextElement();
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
            return InetAddress.getLocalHost();
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
        if (StringUtils.isBlank(ip)) {
            InetAddress inetAddress = getHostLANAddress();
            if (inetAddress != null) {
                ip = inetAddress.getHostAddress();
            }
        }
        return ip;
    }

    /**
     * 获取本机的hostname
     *
     * @return hostname
     */
    public static String getHostName() {
        if (StringUtils.isBlank(hostname)) {
            InetAddress inetAddress = getHostLANAddress();
            if (inetAddress != null) {
                hostname = inetAddress.getHostName();
            }
        }
        return hostname;
    }

    /**
     * 随机获取系统未被使用的端口号
     */
    public static int getRundomPort(int bound) {
        try (ServerSocket socket = new ServerSocket(0)) {
            int port = socket.getLocalPort();
            if (bound > 0) {
                return port + new Random().nextInt(bound);
            } else {
                return port;
            }
        } catch (Exception e) {
            return new Random().nextInt(65535);
        }
    }

    /**
     * 获取当前进程的pid
     *
     * @return pid
     */
    public static String getPid() {
        if (StringUtils.isBlank(pid)) {
            pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
        }
        return pid;
    }

    /**
     * 判断当前运行环境是否为linux
     *
     * @return
     */
    public static boolean isLinux() {
        String os = System.getProperty(OSNAME);
        return os.toLowerCase().startsWith("windows") || os.toLowerCase().contains("mac");
    }

    /**
     * 判断当前运行环境是否为windows
     */
    public static boolean isWindows() {
        String os = System.getProperty(OSNAME);
        return os.toLowerCase().startsWith("windows");
    }

    /**
     * 判断当前是否运行在本地环境下
     * 本地环境包括：Windows、Mac OS
     */
    public static boolean isLocal() {
        return isWindows() || isMac();
    }

    /**
     * 是否为mac os环境
     */
    public static boolean isMac() {
        String os = System.getProperty(OSNAME);
        return os.toLowerCase().contains("mac");
    }

}
