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

package com.zto.fire.flink.anno;

import org.apache.flink.streaming.api.CheckpointingMode;

import java.lang.annotation.*;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.*;

/**
 * 基于注解flink checkpoint配置，优先级低于配置文件，高于@Config注解
 * 注：@Checkpoint中相关时间单位均为秒
 *
 * @author ChengLong 2022-04-26 11:16:00
 * @since 2.2.2
 */
@Inherited
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Checkpoint {

    /**
     * checkpoint周期（s）
     */
    int value() default -1;

    /**
     * checkpoint周期（s），同value
     */
    int interval() default -1;

    /**
     * checkpoint超时时间（s）
     */
    int timeout() default -1;

    /**
     * 是否开启非对齐的checkpoint
     */
    boolean unaligned() default true;

    /**
     * checkpoint的并发度
     */
    int concurrent() default -1;

    /**
     * 两次checkpoint的最短时间间隔
     */
    int pauseBetween() default -1;

    /**
     * 运行checkpoint失败的总次数
     */
    int failureNumber() default -1;

    /**
     * checkpoint的模式
     */
    CheckpointingMode mode() default CheckpointingMode.EXACTLY_ONCE;

    /**
     * 当任务停止时checkpoint的保持策略
     */
    ExternalizedCheckpointCleanup cleanup() default ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

}