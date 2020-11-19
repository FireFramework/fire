package com.zto.fire.common.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * HBase相关的配置
 * @author ChengLong 2020-11-16 16:03:08
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface HConfig {

    /**
     * 是否允许空字段插入HBase
     */
    boolean nullable() default true;

    /**
     * 是否以多版本方式插入
     * 注：fire中将数据转为json后以多版本方式插入，因此多列数据最终存放到HBase中只是一列json数据
     */
    boolean multiVersion() default false;
}
