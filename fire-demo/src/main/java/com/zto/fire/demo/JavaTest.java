package com.zto.fire.demo;


import com.zto.fire.common.util.ValueUtils;

/**
 * 用于测试Java代码
 *
 * @author ChengLong 2019-9-4 13:39:36
 */
public class JavaTest {

    public static void test(Integer id, String name) {
        System.out.println(ValueUtils.isExistsNotEmpty(new Object[] {id, name}));
    }

}