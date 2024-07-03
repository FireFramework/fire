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

package com.zto.fire.common.enu;

/**
 * 系统容错级别：fire.error.tolerance.level
 *
 * @author ChengLong 2024-07-03 13:27:50
 * @since 2.5.0
 */
public enum ErrorTolerance {
    NONE,   // 关闭容错
    TASK,   // 任务级别容错
    STAGE,  // 阶段级别容错
    JOB,    // 作业级别容错
    CONTAINER,  // 容器级别容错
    MASTER;   // 主节点driver/JobManager级别容错
}