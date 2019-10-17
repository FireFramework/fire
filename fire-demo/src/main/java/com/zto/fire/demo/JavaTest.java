package com.zto.fire.demo;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * 用于测试Java代码
 * @author ChengLong 2019-9-4 13:39:36
 */
public class JavaTest {
    public static void main(String[] args) throws Exception {
        List<String> list = new ArrayList<>();
        while (true) {
            list.add(UUID.randomUUID().toString());
        }
    }
}