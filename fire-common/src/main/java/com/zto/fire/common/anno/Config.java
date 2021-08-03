package com.zto.fire.common.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 基于注解进行任务的配置，支持纯注解方式进行任务的参数配置以及指定多个配置文件
 *
 * @author ChengLong 2021-8-3 10:49:30
 * @since 2.1.1
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Config {
    /**
     * 配置文件名称列表
     */
    String[] value() default "";

    /**
     * 配置项列表，key=value的字符串形式
     */
    String[] props() default "";
}