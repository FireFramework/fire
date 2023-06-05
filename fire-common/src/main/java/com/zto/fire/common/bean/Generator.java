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

import java.util.Random;

/**
 * Java Bean生成接口，限定子类必须实现generat方法生成对应的JavaBean实例
 *
 * @author ChengLong 2023-06-05 10:28:38
 * @since 2.3.6
 */
public interface Generator<T> {

    /**
     * JavaBean生成方法，子类需复写该接口，并实现JavaBean中一些field的set逻辑
     */
    T generate();
}
