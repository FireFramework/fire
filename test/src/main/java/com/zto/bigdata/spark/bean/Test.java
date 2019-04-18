package com.zto.bigdata.spark.bean;

import com.zto.bigdata.spark.common.anno.Rest;
import com.zto.bigdata.spark.common.util.ReflectionUtils;

import java.util.List;

public class Test {
    public static void main(String[] args) throws Exception {
        List<Class<?>> lists = ReflectionUtils.scanAnnotation("com.zto", Rest.class);
        for (Class clazz : lists) {
            System.out.println(clazz.getSimpleName());
        }
    }
}
