package com.zto.fire.common.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 用于标识单元测试的测试步骤
 *
 * @author ChengLong 2020-11-13 09:39:28
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface TestStep {

    /**
     * 测试步骤
     */
    int step() default 1;

    /**
     * 用于单元测试描述
     */
    String desc() default "单元测试说明";
}
