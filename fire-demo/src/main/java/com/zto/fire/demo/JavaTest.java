package com.zto.fire.demo;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * 用于测试Java代码
 * @author ChengLong 2019-9-4 13:39:36
 */
public class JavaTest {
    static {
        System.setProperty("java.rmi.server.hostname", "10.9.46.112");
        System.setProperty("com.sun.management.jmxremote.port", "9001");
        System.setProperty("com.sun.management.jmxremote.ssl", "false");
        System.setProperty("com.sun.management.jmxremote.authenticate", "false");
        System.out.println("完成setProperty");
    }

    public static void main(String[] args) throws Exception {
        System.out.println("开始main方法：" + System.getProperty("java.rmi.server.hostname"));
        List<String> list = new ArrayList<>();
        while (true) {
            list.add(UUID.randomUUID().toString());
            Thread.sleep(1000);
        }
    }
}