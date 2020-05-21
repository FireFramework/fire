package com.zto.fire.common.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 用于标识方法为udf函数
 * 在方法上标记该注解，可自动被注册为Spark sql的udf函数
 *
 * @author ChengLong 2019年12月20日 13:32:06
 * @since 0.4.1
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface UDF {

    /**
     * udf函数名称，若与方法名不一致，则使用value指定
     */
    String value() default "";
}
