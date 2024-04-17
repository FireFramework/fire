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

package com.zto.fire.common.bean;

import java.util.Set;

/**
 * 用于封装计算引擎消费的消息队列相关信息
 *
 * @author ChengLong
 * @version 2.4.5
 * @Date 2024/4/17 13:14
 */
public class JobConsumerInfo {
    private FireTask taskInfo;
    private Set<ConsumerOffsetInfo> offsetInfo;

    public JobConsumerInfo() {
    }

    public JobConsumerInfo(FireTask taskInfo, Set<ConsumerOffsetInfo> offsetInfo) {
        this.taskInfo = taskInfo;
        this.offsetInfo = offsetInfo;
    }

    public FireTask getTaskInfo() {
        return taskInfo;
    }

    public void setTaskInfo(FireTask taskInfo) {
        this.taskInfo = taskInfo;
    }

    public Set<ConsumerOffsetInfo> getOffsetInfo() {
        return offsetInfo;
    }

    public void setOffsetInfo(Set<ConsumerOffsetInfo> offsetInfo) {
        this.offsetInfo = offsetInfo;
    }
}
