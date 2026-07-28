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

package com.zto.fire.common.util;

/**
 * 高性能 Bean 属性拷贝器接口
 * <p>
 * 实现类由 {@link BeanCopierFactory} 通过 ByteBuddy 在运行时生成：
 * {@code copy} 方法体内是对已有 getter/setter 的直接调用，而不是再生成一套 get/set，也不走反射
 * </p>
 *
 * @author ChengLong
 * @since 3.0.0
 */
public interface BeanCopier {

    /**
     * 将 source 中与 target 同名且类型兼容的属性拷贝到 target
     *
     * @param source 源对象
     * @param target 目标对象
     */
    void copy(Object source, Object target);
}
