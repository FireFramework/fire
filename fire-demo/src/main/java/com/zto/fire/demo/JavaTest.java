package com.zto.fire.demo;


import com.zto.fire.common.anno.Scheduled;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

/**
 * 用于测试Java代码
 *
 * @author ChengLong 2019-9-4 13:39:36
 */
public class JavaTest {
    @Scheduled(cron = "0/1 * * * * ?")
    public void test() {
        System.err.println("java test==========");
    }
}