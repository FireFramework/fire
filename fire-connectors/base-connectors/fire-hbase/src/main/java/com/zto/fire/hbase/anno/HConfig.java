/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zto.fire.hbase.anno;

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

    /**
     * 默认获取的版本数
     */
    int versions() default 1;

    /**
     * BulkLoad 的 HFile staging 目录前缀（不含表名后缀）
     * 优先级低于配置 fire.hbase.bulkload.stagingDir，高于方法入参
     */
    String bulkLoadStagingDir() default "";

    /**
     * BulkLoad 前后是否删除 staging 目录
     * 优先级低于配置 fire.hbase.bulkload.deleteStagingDir，默认 false（不删除）
     */
    boolean bulkLoadDeleteStagingDir() default false;
}
