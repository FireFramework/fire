package com.zto.fire.demo;


import com.google.common.collect.Lists;
import com.zto.fire.common.anno.Scheduled;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.List;

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

    public static void getVarGeneractType(Object obj) {

    }

    public static void main(String[] args) throws Exception {
        List<Integer> list = Lists.newArrayList();
        getVarGeneractType(list);
        Method method = JavaTest.class.getMethod("getVarGeneractType", Object.class);
        System.out.println(method.getGenericParameterTypes()[0].getClass().getName());
    }
}