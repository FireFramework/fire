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

}
