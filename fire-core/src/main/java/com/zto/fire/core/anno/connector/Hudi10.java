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
public @interface Hudi10 {

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

    /**
     * 当使用布隆索引时是否开启基于Bucketized的仿数据倾斜
     */
    boolean useBloomIndexBucketized() default true;

    /**
     * 布隆索引并行度，优先级高于parallelism
     */
    int bloomIndexParallelism() default -1;

    /**
     * 布隆过滤器每个bucket的key数
     */
    int bloomkeysPerBucket() default -1;

    /**
     * 是否启用异步clean
     */
    boolean cleanerAsync() default true;

    /**
     * clean的策略：KEEP_LATEST_FILE_VERSIONS、KEEP_LATEST_COMMITS
     */
    String cleanerPolicy() default "KEEP_LATEST_FILE_VERSIONS";

    /**
     * clean保留的版本数
     */
    int cleanerCommitsRetained() default -1;

    /**
     * 是否开启记录级索引（> hudi 0.14.0）
     * hoodie.metadata.record.index.enable=true 和 hoodie.index.type=RECORD_INDEX
     */
    boolean useRecordIndex() default false;

    /**
     * 是否使用hbase index
     * HbaseIndex 相关参数
     *
     * hoodie.index.type=HBASE
     * hoodie.index.hbase.zkport=2181
     * hoodie.index.hbase.zkquorum=hzpl004138023-hadoop-zh.ztosys.com,hzpl004138041-hadoop-zh.ztosys.com,hzpl004138024-hadoop-zh.ztosys.com,hzpl004138042-hadoop-zh.ztosys.com,hzpl004138051-hadoop-zh.ztosys.com
     * hoodie.index.hbase.zknode.path=/hbase
     * hoodie.index.hbase.table=hudi:xxxxx
     * hoodie.index.hbase.get.batch.size=2000
     * hoodie.index.hbase.put.batch.size=2000
     * hoodie.index.hbase.put.batch.size.autocompute=true
     * hoodie.index.hbase.qps.fraction=0.5
     * hoodie.index.hbase.max.qps.per.region.server=50000
     * hoodie.index.hbase.qps.allocator.class=org.apache.hudi.index.hbase.DefaultHBaseQPSResourceAllocator
     * hoodie.index.hbase.rollback.sync=true
     * hoodie.hbase.index.update.partition.path=true
     *
     */
    boolean useHbaseIndex() default false;

    /**
     * hbase默认连接端口
     * @return
     */
    int hbasePort() default 2181;

    /**
     * hbase 连接地址
     * @return
     */
    String hbaseZkQuorum() default "";

    /**
     * hbasezkNode路径
     * @return
     */
    String hbaseZkNodePath() default  "/hbase";

    /**
     * hbase 中tableName
     * @return
     */
    String hbaseTable() default "";

    /**
     * get操作的批次大小
     * @return
     */
    long hbaseGetBatchSize() default 1000;

    /**
     * put操作的批次大小
     * @return
     */
    long hbasePutBatchSize() default 1000;

    /**
     *
     * @return
     */
    boolean hbasePutBatchSizeAutoCompute() default false;

    /**
     * Property to set maximum QPS allowed per Region Server
     * should be same across various jobs
     * @return
     */
    long hbaseMaxQpsPerRegionServer() default 50000;

    /**
     * set the fraction of the global share of QPS that should be allocated to this job
     * @return
     */
    float hbaseQpsFraction() default 0.2f;

    /**
     * rollback method will delete the last failed task index
     * @return
     */
    boolean hbaseRollbackSync() default true;

    /**
     * delete old record in old paritition insert in new partition
     * @return
     */
    boolean hbaseUpdatePartitionPath() default true;

    /**
     * 默认hbaseQpsAllocatorClass
     * @return
     */
    String hbaseQpsAllocatorClass() default "org.apache.hudi.index.hbase.DefaultHBaseQPSResourceAllocator";
    /**
     * hbase Index User
     */
    String hbaseIndexUser() default "";
}