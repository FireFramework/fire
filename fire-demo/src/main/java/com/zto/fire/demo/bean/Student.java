package com.zto.fire.demo.bean;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.zto.fire.common.bean.HBaseBaseBean;
import com.zto.fire.common.util.DateFormatUtils;
import com.zto.fire.common.util.EncryptUtils;
import com.zto.fire.common.util.JSONUtils;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * 对应HBase表的JavaBean
 *
 * @author ChengLong 2019-6-20 16:06:16
 */
public class Student extends HBaseBaseBean<Student> {
    private Long id;
    private String name;
    private Integer age;
    // 多列族情况下需使用family单独指定
    private String createTime;
    // 若JavaBean的字段名称与HBase中的字段名称不一致，需使用value单独指定
    // 此时hbase中的列名为length1，而不是length
    // @FieldName(family = "info", value = "length1")
    private BigDecimal length;
    private Boolean sex;

    /**
     * rowkey的构建
     *
     * @return
     */
    @Override
    public Student buildRowKey() {
        this.rowKey = this.id.toString();
        return this;
    }

    public Student(Long id, String name, Integer age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    public Student(Long id, String name, Integer age, BigDecimal length, Boolean sex, String createTime) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.length = length;
        this.sex = sex;
        this.createTime = createTime;
    }

    public Student(Long id, String name, Integer age, BigDecimal length) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.length = length;
    }

    public Student() {
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public BigDecimal getLength() {
        return length;
    }

    public void setLength(BigDecimal length) {
        this.length = length;
    }

    public Boolean getSex() {
        return sex;
    }

    public void setSex(Boolean sex) {
        this.sex = sex;
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

    @Override
    public String toString() {
        return JSON.toJSONString(this, SerializerFeature.WriteNullListAsEmpty);
    }

    public static List<Student> newStudentList() {
        String dateTime = DateFormatUtils.formatCurrentDateTime();
        return Arrays.asList(
                new Student(1L, "admin", 12, new BigDecimal(12.1), true, dateTime),
                new Student(2L, "root", 22, new BigDecimal(22), true, dateTime),
                new Student(3L, "scala", 11, new BigDecimal(11), true, dateTime),
                new Student(4L, "spark", 15, new BigDecimal(15), true, dateTime),
                new Student(5L, "java", 16, new BigDecimal(16.1), true, dateTime),
                new Student(6L, "hive", 17, new BigDecimal(17.1), true, dateTime),
                new Student(7L, "presto", 18, new BigDecimal(18.1), true, dateTime),
                new Student(8L, "flink", 19, new BigDecimal(19.1), true, dateTime),
                new Student(9L, "streaming", 10, new BigDecimal(10.1), true, dateTime),
                new Student(10L, "sql", 12, new BigDecimal(12.1), true, dateTime)
        );
    }

    /**
     * 构建student集合
     *
     * @return
     */
    public static List<Student> buildStudentList() {
        List<Student> studentList = new LinkedList<>();
        try {
            for (int i = 1; i <= 1; i++) {
                Thread.sleep(500);
                Student stu = new Student(1L, "root", i + 1, new BigDecimal(1 + i), true, DateFormatUtils.formatCurrentDateTime());
                studentList.add(stu);
            }

            for (int i = 1; i <= 2; i++) {
                Thread.sleep(500);
                Student stu = new Student(2L, "admin", i + 2, new BigDecimal(2019.05180919 + i), false, DateFormatUtils.formatCurrentDateTime());
                studentList.add(stu);
            }

            for (int i = 1; i <= 3; i++) {
                Thread.sleep(500);
                Student stu = new Student(3L, "spark", i + 3, new BigDecimal(33.1415926 + i));
                studentList.add(stu);
            }

            for (int i = 1; i <= 3; i++) {
                Thread.sleep(500);
                Student stu = new Student(4L, "flink", i + 4, new BigDecimal(4.2 + i), true, DateFormatUtils.formatCurrentDateTime());
                studentList.add(stu);
            }

            for (int i = 1; i <= 3; i++) {
                Thread.sleep(500);
                Student stu = new Student(5L, "hadoop", i + 5, new BigDecimal(5.5 + i), false, DateFormatUtils.formatCurrentDateTime());
                studentList.add(stu);
            }
            for (int i = 1; i <= 3; i++) {
                Thread.sleep(500);
                Student stu = new Student(6L, "hbase", i + 6, new BigDecimal(66.66 + i), true, DateFormatUtils.formatCurrentDateTime());
                studentList.add(stu);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return studentList;
    }

    public static void main(String[] args) throws Exception {
        String json = "{\"className\": \"com.zto.bigdata.spark.zrc.ZrcDemo\", \"url\": \"http://10.9.39.10:8089\", \"fireVersion\": \"0.10\", \"id\": 10, \"clear1\": true}";
        System.out.println(JSONUtils.getValue(json, "clear", false));
        System.out.println(EncryptUtils.md5Encrypt("($zto%-%fire$)" + "com.zto.fire.demo.Test" + DateFormatUtils.formatCurrentDate()));
    }
}
