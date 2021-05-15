package com.zto.fire.examples.bean;

import java.math.BigDecimal;
import java.util.LinkedList;
import java.util.List;

public class People {
    private Long id;
    private String name;
    private Integer age;
    private Double length;
    private BigDecimal data;

    public People() {
    }

    public People(Long id, String name, Integer age, Double length, BigDecimal data) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.length = length;
        this.data = data;
    }

    public static List<People> createList() {
        List<People> list = new LinkedList<>();
        for (int i=0; i<10; i++) {
            list.add(new People((long) i, "admin_" + i, i, i * 0.1, new BigDecimal(i * 10.1012)));
        }
        return list;
    }
}
