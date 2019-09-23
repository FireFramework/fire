package com.zto.fire.demo;

import com.zto.fire.demo.bean.Student;

import java.util.HashSet;
import java.util.Set;

/**
 * 用于测试Java代码
 * @author ChengLong 2019-9-4 13:39:36
 */
public class JavaTest {
    public static void main(String[] args) {
        Set<Student> set = new HashSet<Student>();
        set.add(new Student(1L, "root", 2));
        set.add(new Student(2L, "admin", 3));
        Set<Student> set2 = new HashSet<Student>();
        set2.addAll(set);
        set.clear();
        System.out.println(set.size() + " set2.size=" + set2.size());
    }
}
