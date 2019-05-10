package com.zto.bigdata.spark.bean;

import com.zto.bigdata.spark.common.bean.HBaseBaseBean;
import com.zto.bigdata.spark.common.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.Map;

public class Test {
    public static void main(String[] args) throws Exception {
        Map<String, Field> map = ReflectionUtils.getAllFields(Student.class);
        Map<String, Field> map1 = ReflectionUtils.getAllFields(Student.class);
        Map<String, Field> map2 = ReflectionUtils.getAllFields(HBaseBaseBean.class);
        ReflectionUtils.getAllMethods(Student.class);
        ReflectionUtils.getAllMethods(HBaseBaseBean.class);

        System.out.println(map.size());
    }
}
