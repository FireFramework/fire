package com.zto.bigdata.spark.bean;

import com.zto.bigdata.spark.common.util.IOUtils;

import java.io.InputStream;
import java.util.Properties;

public class Test {
    public static void main(String[] args) throws Exception {
        InputStream resource = Test.class.getClassLoader().getResourceAsStream("default.properties");
        Properties props = new Properties();
        props.load(resource);
        IOUtils.close(resource);
        System.out.println(props.getProperty("spark.kafka.brokers.url"));
    }
}
