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

import com.zto.fire.common.util.*;

/**
 * 用于封装Fire框架任务的基本信息
 *
 * @author ChengLong 2022-08-30 14:44:03
 * @since 2.3.2
 */
public class FireTask {

    /**
     * 触发异常的执行引擎：spark/flink
     */
    protected String engine;

    /**
     * 异常所在jvm进程发送的主机ip
     */
    protected String ip;

    /**
     * 异常所属jvm进程所在的主机名称
     */
    protected String hostname;

    /**
     * 进程的pid
     */
    protected String pid;

    /**
     * 任务的主类名：package+类名
     */
    protected String mainClass;

    /**
     * 异常发生的时间戳
     */
    protected String timestamp;

    public FireTask() {
        this.engine = FireUtils.engine();
        this.ip = OSUtils.getIp();
        this.timestamp = DateFormatUtils.formatCurrentDateTime();
        this.mainClass = FireUtils.mainClass();
        this.hostname = OSUtils.getHostName();
        this.pid = OSUtils.getPid();
    }

    public String getEngine() {
        return engine;
    }

    public void setEngine(String engine) {
        this.engine = engine;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public String getMainClass() {
        return mainClass;
    }

    public void setMainClass(String mainClass) {
        this.mainClass = mainClass;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }
}
