package com.zto.bigdata.spark.bean;

import com.zto.bigdata.spark.common.bean.HBaseBaseBean;

import java.util.Arrays;
import java.util.List;

public class Student extends HBaseBaseBean<Student> {
    private Long id;
    private String name;
    private Integer age;

    /**
     * rowkey的构建
     * @return
     */
    @Override
    public Student buildRowKey() {
        this.rowKey = this.id + this.name;
        return this;
    }

    public Student(Long id, String name, Integer age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    public Student() {
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public static List<Student> newStudentList() {
        return Arrays.asList(new Student(1L, "admin", 12),
                new Student(1L, "admin", 12),
                new Student(2L, "root", 22),
                new Student(3L, "scala", 11),
                new Student(4L, "spark", 15),
                new Student(5L, "java", 16),
                new Student(6L, "hive", 17),
                new Student(7L, "presto", 18),
                new Student(8L, "flink", 19),
                new Student(9L, "streaming", 10),
                new Student(10L, "sql", 12)
                );
    }
}
