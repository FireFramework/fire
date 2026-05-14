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

package com.zto.fire.core.plugin;

import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.agent.builder.ResettableClassFileTransformer;

import java.lang.instrument.Instrumentation;

/**
 * ByteBuddy增强管理器基类，统一维护Agent安装、卸载与Advice ClassLoader处理逻辑
 *
 * @author ChengLong
 * @since 3.0.0
 */
public abstract class TraceManager {
    protected TraceManager() {
    }

    /**
     * 安装ByteBuddy Agent并返回当前JVM的Instrumentation实例
     */
    protected static Instrumentation installByteBuddyAgent() {
        System.setProperty("jdk.attach.allowAttachSelf", "true");
        ByteBuddyAgent.install();
        return ByteBuddyAgent.getInstrumentation();
    }

    /**
     * 获取当前JVM的Instrumentation实例
     */
    protected static Instrumentation getInstrumentation() {
        return ByteBuddyAgent.getInstrumentation();
    }

    /**
     * 创建默认AgentBuilder，保持各类增强的ByteBuddy基础配置一致
     */
    protected static AgentBuilder newDefaultAgentBuilder() {
        return new AgentBuilder.Default()
                .disableClassFormatChanges()
                .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION)
                .with(AgentBuilder.Listener.StreamWriting.toSystemError().withErrorsOnly());
    }

    /**
     * 创建Advice转换器，并加入可定位Advice类的ClassLoader
     */
    protected static AgentBuilder.Transformer.ForAdvice newAdviceTransformer(Class<?> adviceClass) {
        AgentBuilder.Transformer.ForAdvice advice = new AgentBuilder.Transformer.ForAdvice();
        ClassLoader adviceClassLoader = adviceClass.getClassLoader();
        if (adviceClassLoader != null) {
            advice = advice.include(adviceClassLoader);
        }

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        if (contextClassLoader != null) {
            advice = advice.include(contextClassLoader);
        }

        return advice;
    }

    /**
     * reset指定的ClassFileTransformer，仅卸载调用方持有的增强
     */
    protected static void resetTransformer(ResettableClassFileTransformer transformer) {
        if (transformer != null) {
            transformer.reset(getInstrumentation(), AgentBuilder.RedefinitionStrategy.RETRANSFORMATION);
        }
    }
}
