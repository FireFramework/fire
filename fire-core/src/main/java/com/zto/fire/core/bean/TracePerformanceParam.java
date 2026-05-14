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

package com.zto.fire.core.bean;

import java.util.List;

/**
 * POST /system/trace/performance 请求体
 *
 * @author ChengLong
 * @since 3.0.0
 */
public class TracePerformanceParam {
    private String command;
    private Boolean distribute;
    private String ip;
    private List<TracePerformanceTarget> targets;

    public TracePerformanceParam() {
    }

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public Boolean getDistribute() {
        return distribute;
    }

    public void setDistribute(Boolean distribute) {
        this.distribute = distribute;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public List<TracePerformanceTarget> getTargets() {
        return targets;
    }

    public void setTargets(List<TracePerformanceTarget> targets) {
        this.targets = targets;
    }
}
