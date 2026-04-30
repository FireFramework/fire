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

/**
 * 用于承载或解析Trace相关restful参数
 *
 * @author ChengLong
 * @since 3.0.0
 */
public class TraceParam {
    private String command;
    private Boolean distribute;
    private String ip;
    private String className;
    private Long thresholdMs;

    public TraceParam() {
    }

    public TraceParam(String command, Boolean distribute, String ip, String className, Long thresholdMs) {
        this.command = command;
        this.distribute = distribute;
        this.ip = ip;
        this.className = className;
        this.thresholdMs = thresholdMs;
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

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public Long getThresholdMs() {
        return thresholdMs;
    }

    public void setThresholdMs(Long thresholdMs) {
        this.thresholdMs = thresholdMs;
    }
}
