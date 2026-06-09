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

/**
 * ByteBuddy代码规范检测Advice
 *
 * @author ChengLong
 * @since 3.0.0
 */
public final class TraceStandardAdvice {
    private TraceStandardAdvice() {
    }

    @Advice.OnMethodEnter(suppress = Throwable.class)
    static void enter(@Advice.Origin("#t") String declaringType,
                      @Advice.Origin("#m") String methodName) {
        try {
            // 对于需要拦截的方法，只分析一次堆栈，降低开销
            if (!TraceStandardManager.shouldAnalyze(declaringType, methodName)) {
                return;
            }

            // 分析执行堆栈，找出不规范的代码
            TraceStandardManager.analyzeCodeStandard(declaringType, methodName);
        } catch (Throwable ignored) {
            // Spark 等引擎中第三方类与 fire-core 可能不在同一 ClassLoader，检测失败时不影响业务调用
        }
    }
}
