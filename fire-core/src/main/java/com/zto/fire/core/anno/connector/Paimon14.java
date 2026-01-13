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

package com.zto.fire.core.anno.connector;

import java.lang.annotation.*;

/**
 * 基于注解进行paimon参数配置，支持纯注解方式进行任务的参数配置以及指定多个配置文件
 *
 * @author ChengLong 2024-09-14 09:49:57
 * @since 2.5.0
 */
@Inherited
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Paimon14 {

    /**
     * 配置项列表，key=value的字符串形式
     */
    String[] props() default "";

    /**
     * 配置的字符串
     */
    String value() default "";

    /**
     * 数据源标识，用于区分不同的flink sql数据源，对应create table with中的datasource
     */
    String datasource();

    /**
     * paimon分区TTL时间：partition.expiration-time=31d
     */
    String partitionTTL() default "";

    /**
     * paimon分区格式：partition.timestamp-formatter=yyyyMMdd
     */
    String partitionFormat() default "";

    /**
     * 是否在触发savepoint时生成tag：sink.savepoint.auto-tag=true
     */
    boolean tagOnSavepoint() default false;

    /**
     * 是否自动创建tag：tag.automatic-creation=watermark
     */
    boolean tagAutoCreate() default false;

    /**
     * tag TTL时间：tag.default-time-retained=3d
     */
    String tagTTL() default "";

    /**
     * tag保留的最大数量：tag.num-retained-max=10
     */
    int tagNumMax() default -1;

    /**
     * tag创建周期：tag.creation-period=daily
     */
    String tagCreationPeriod() default "";

    /**
     * 默认的合并引擎：merge-engine=deduplicate
     */
    String mergeEngine() default "deduplicate";

    /**
     * bucket数量:bucket=128
     */
    long bucket();

    /**
     * 默认的文件存储格式：file.format=parquet
     */
    String fileFormat() default "parquet";

    /**
     * 是否开启离线异步合并（生产环境建议开启）：write-only=true
     */
    boolean writeOnly() default false;

    /**
     * 快照TTL时间：snapshot.time-retained=3d
     */
    String snapshotTTL() default "";

    /**
     * 最小快照数量：snapshot.num-retained.min=480
     */
    long snapshotNumMin() default -1;

    /**
     * snapshot.num-retained.max=2000
     */
    long snapshotNumMax() default -1;

    /**
     * 快照过期时间版本数限制：snapshot.expire-limit=1000
     */
    long snapshotExpireLimit() default -1;

    /**
     * 本地合并会在输入记录被 bucket 洗牌并写入 sink 之前对其进行缓冲和合并。 缓冲区满后将被刷新。 主要用于解决主键的数据偏移问题。 我们建议在试用此功能时，从 64 MB 开始：
     * local-merge-buffer-size=64mb
     */
    String mergeBufferSize() default "";

    /**
     * 触发压缩的排序运行编号。 包括 0 级文件（一个文件一个排序运行）和高级别运行（一个级别一个排序运行）：num-sorted-run.compaction-trigger=20
     */
    long compactionTrigger() default -1;

    /**
     * 在转换为分类磁盘文件之前，内存中需要存储的数据量：write-buffer-size=512mb
     */
    String writeBufferSize() default "";
}