package com.zto.bigdata.spark.common.anno;

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
     * @return
     */
    String value() default "";

    /**
     * 列族名称
     * @return
     */
    String family() default "info";

    /**
     * 是否映射Hive与HBase，默认映射
     * @return
     */
    boolean mapping() default true;

    /**
     * 不使用该字段，默认为使用
     * @return
     */
    boolean disuse() default false;

    /**
     * 是否可以为空
     * @return
     */
    boolean nullable() default true;

    /**
     * 是否为主键字段
     * @return
     */
    boolean id() default false;

    /**
     * HBase表的命名空间
     * @return
     */
    String namespace() default "default";

    /**
     * 表名
     * @return
     */
    String tableName() default "test";

    /**
     * 字段注释
     * @return
     */
    String comment() default "";
}
