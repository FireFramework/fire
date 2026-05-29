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

package com.zto.fire.common.bean.standard;

import com.zto.fire.common.bean.FireTask;

import java.io.Serializable;

/**
 * 用于封装 TraceStandard 采集到的代码规范检测信息
 *
 * @author ChengLong
 * @since 3.0.0
 */
public class Standard extends FireTask implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * 不建议使用的原生API包名
     */
    private String apiPackage;

    /**
     * 不建议使用的原生API类名
     */
    private String apiClass;

    /**
     * 不建议使用的原生API方法名
     */
    private String apiMethod;

    /**
     * 触发不规范调用的业务类名
     */
    private String callerClass;

    /**
     * 触发不规范调用的业务方法名
     */
    private String callerMethod;

    /**
     * 触发不规范调用的代码行号
     */
    private Integer lineNumber;

    /**
     * 建议替换使用的Fire封装API
     */
    private String suggestion;

    public Standard() {
        super();
    }

    public Standard(String apiPackage, String apiClass, String apiMethod, String callerClass, String callerMethod, Integer lineNumber, String suggestion) {
        this.apiPackage = apiPackage;
        this.apiClass = apiClass;
        this.apiMethod = apiMethod;
        this.callerClass = callerClass;
        this.callerMethod = callerMethod;
        this.lineNumber = lineNumber;
        this.suggestion = suggestion;
    }

    public String getApiPackage() {
        return apiPackage;
    }

    public void setApiPackage(String apiPackage) {
        this.apiPackage = apiPackage;
    }

    public String getApiClass() {
        return apiClass;
    }

    public void setApiClass(String apiClass) {
        this.apiClass = apiClass;
    }

    public String getApiMethod() {
        return apiMethod;
    }

    public void setApiMethod(String apiMethod) {
        this.apiMethod = apiMethod;
    }

    public String getCallerClass() {
        return callerClass;
    }

    public void setCallerClass(String callerClass) {
        this.callerClass = callerClass;
    }

    public String getCallerMethod() {
        return callerMethod;
    }

    public void setCallerMethod(String callerMethod) {
        this.callerMethod = callerMethod;
    }

    public Integer getLineNumber() {
        return lineNumber;
    }

    public void setLineNumber(Integer lineNumber) {
        this.lineNumber = lineNumber;
    }

    public String getSuggestion() {
        return suggestion;
    }

    public void setSuggestion(String suggestion) {
        this.suggestion = suggestion;
    }
}
