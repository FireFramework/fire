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
 * 按名称取值的函数式源接口
 * <p>
 * 用于将非标准 JavaBean（如仅提供 {@code getObject(String)} 的数据结构）
 * 作为 {@link BeanUtils#copyProperties(NamedValueSource, Object)} 的拷贝源，
 * 而无需 fire-common 依赖具体实现类型。
 * </p>
 * <p>
 * 典型用法：{@code BeanUtils.copyProperties(inputData::getObject, targetClass)}
 * </p>
 *
 * @author ChengLong
 * @since 3.0.0
 */
@FunctionalInterface
public interface NamedValueSource {

    /**
     * 按属性名取值
     *
     * @param name 属性名（与目标 JavaBean 属性名一致）
     * @return 对应值，不存在时可返回 null
     */
    Object getObject(String name);
}
