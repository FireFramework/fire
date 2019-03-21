package com.zto.bigdata.spark.bean;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Test {
    public static void main(String[] args) {
        List<String> list = Arrays.asList("1", "2", "3");
        List<String> sub = list.subList(1, list.size());
        Collections.shuffle(list);
        for(String str : list) {
            System.out.println(str);
            break;
        }
    }
}
