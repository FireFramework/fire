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
 * 基于注解进行hudi参数配置，支持纯注解方式进行任务的参数配置以及指定多个配置文件
 *
 * @author ChengLong 2023-03-28 17:11:06
 * @since 2.3.5
 */
@Inherited
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Hudi7 {

    /**
     * 配置项列表，key=value的字符串形式
     */
    String[] props() default "";

    /**
     * 配置的字符串
     */
    String value() default "";

    /**
     * hudi相关并行度
     */
    int parallelism() default -1;

    /**
     * 几个批次做一次compaction，当大于零时默认开启inline的compaction
     * 相当于：
     * hoodie.compact.inline=true
     * hoodie.compact.inline.max.delta.commits=xxx
     */
    int compactCommits() default 5;

    /**
     * 是否只做compaction的调度计划，适用于有独立的离线的compaction任务场景下开启
     * 当开启该调度计划时，默认关闭inline的异步compaction：
     * hoodie.compact.inline=false
     * hoodie.compact.schedule.inline=true
     */
    boolean compactSchedule() default false;

    /**
     * 几个批次做一次clustering，当大于零时默认开启inline的clustering
     * 相当于：
     * hoodie.clustering.inline=true
     * hoodie.clustering.inline.max.commits=xxx
     */
    int clusteringCommits() default -1;

    /**
     * 是否只做clustering的调度计划，适用于有独立的离线的clustering任务场景下开启
     * 当开启该调度计划时，默认关闭inline的异步clustering：
     * hoodie.clustering.inline=false
     * hoodie.clustering.schedule.inline=true
     */
    boolean clusteringSchedule() default false;

    /**
     * clustering的字段列表：hoodie.clustering.plan.strategy.sort.columns
     */
    String clustringColumns() default "";

    /**
     * 基于多少个分区进行clustering执行计划的生成：hoodie.clustering.plan.strategy.daybased.lookback.partitions
     */
    int clusteringPartitions() default -1;
}