package com.zto.fire.common.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 用于标识该field对应数据库中的名称
 * Created by ChengLong on 2017-03-15.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.FIELD})
public @interface FieldName {
    /**
     * fieldName，映射到hbase中作为qualifier名称
     */
    String value() default "";

    /**
     * 列族名称
     */
    String family() default "info";

    /**
     * 不使用该字段，默认为使用
     */
    boolean disuse() default false;

    /**
     * 是否可以为空
     */
    boolean nullable() default true;

    /**
     * 是否为主键字段
     * @return
     */
    boolean id() default false;

    /**
     * HBase表的命名空间
     */
    String namespace() default "default";

    /**
     * 字段注释
     */
    String comment() default "";
}
