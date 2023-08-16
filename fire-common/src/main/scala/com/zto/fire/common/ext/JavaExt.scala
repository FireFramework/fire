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

package com.zto.fire.common.ext

import com.zto.fire.predef._

/**
 * Java语法扩展
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2021-01-04 13:50
 */
trait JavaExt {


  /**
   * Java map API扩展
   */
  implicit class MapExt[K, V](map: JMap[K, V]) {

    /**
     * map的get操作，如果map中存在则直接返回，否则会根据fun定义的逻辑进行value的初始化
     * 注：fun中定义的逻辑仅会在key对应的value不存在时被调用一次
     *
     * @param key map的key
     * @param fun 用于定义key对应value的初始化逻辑
     * @return map中key对应的value
     */
    def mergeGet(key: K)(fun: => V): V = {
      requireNonEmpty(key)
      if (!map.containsKey(key)) map.put(key, fun)
      map.get(key)
    }
  }

  /**
   * Java Set API扩展
   * @param set
   * set集合
   */
  implicit class SetExt[V](set: JSet[V]) {

    /**
     * 替代set中指定的对象实例，适用于以下场景：
     * 实例与set中已存在的相互equals，但却是两个不同的对象
     *
     * @param value
     * 待替换的实例
     */
    def replace(value: V): Unit = {
      if (set.contains(value)) {
        set.remove(value)
        set.add(value)
      }
    }
  }
}
