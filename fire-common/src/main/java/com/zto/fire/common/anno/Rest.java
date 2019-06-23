package com.zto.fire.common.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 用于标识启用restful接口
 *
 * @author ChengLong 2019-4-16 11:07:13
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.FIELD})
public @interface Rest {

    /**
     * restful路径名
     *
     * @return
     */
    String value() default "";

    /**
     * 接口访问的方式: GET/POST
     *
     * @return
     */
    String method() default "GET";
}
