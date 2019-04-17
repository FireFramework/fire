package com.zto.bigdata.spark.bean;

import java.net.ServerSocket;

public class Test {
    public static void main(String[] args) throws Exception {
        ServerSocket s = new ServerSocket(0);
        System.out.println("listening on port: " + s.getLocalPort());
        System.out.println("listening on port: " + s.getLocalPort());
        System.out.println("listening on port: " + s.getLocalPort());
        ServerSocket s2 = new ServerSocket(0);
        System.out.println("listening on port: " + s2.getLocalPort());
        System.out.println("listening on port: " + s2.getLocalPort());
        System.out.println("listening on port: " + s2.getLocalPort());
    }
}
