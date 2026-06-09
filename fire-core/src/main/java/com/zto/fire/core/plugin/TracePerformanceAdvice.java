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

import net.bytebuddy.asm.Advice;
import net.bytebuddy.implementation.bytecode.assign.Assigner;

/**
 * ByteBuddy普通方法性能耗时Advice
 *
 * @author ChengLong
 * @since 3.0.0
 */
public final class TracePerformanceAdvice {
    private TracePerformanceAdvice() {
    }

    @Advice.OnMethodEnter(suppress = Throwable.class)
    static long enter() {
        return System.nanoTime();
    }

    @Advice.OnMethodExit(onThrowable = Throwable.class, suppress = Throwable.class)
    static void exit(@Advice.Enter long start,
                     @Advice.Origin("#t") String declaringType,
                     @Advice.Origin("#m") String methodName,
                     @Advice.AllArguments Object[] allArgs,
                     @Advice.Return(readOnly = true, typing = Assigner.Typing.DYNAMIC) Object result,
                     @Advice.Thrown(readOnly = true) Throwable thrown) {
        try {
            TracePerformanceManager.printTracePerformanceLog(start, declaringType, methodName, allArgs, result, thrown);
        } catch (Throwable ignored) {
            // 与 TraceStandard 相同，避免 ClassLoader 隔离导致增强逻辑拖垮业务方法
        }
    }
}
