package com.zto.bigdata.spark.common.util;

/**
 * 环境工具类
 */
public class EnvUtils {

    /**
     * 判断当前运行环境是否为linux
     * @return
     */
    public static boolean isLinux() {
        String os = System.getProperty("os.name");
        if (os.toLowerCase().startsWith("windows")) {
            return false;
        } else {
            return true;
        }
    }

    public static void main(String[] args) {
        System.out.println(isLinux());
    }
}
