package com.zto.fire.spark.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Spark Streaming任务的批次时间
 *
 * @author ChengLong 2021年8月3日19:39:28
 * @since 2.1.1
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface StreamingDuration {
    /**
     * 批次时间，单位秒
     */
    int value() default 10;
}
