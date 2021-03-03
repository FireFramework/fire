package com.zto.fire.common.util

/**
 * Java类型映射
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2020-12-16 15:40
 */
trait TypeMap {
  // Java API库映射
  type JInt = java.lang.Integer
  type JLong = java.lang.Long
  type JBoolean = java.lang.Boolean
  type JChar = java.lang.Character
  type JFloat = java.lang.Float
  type JShort = java.lang.Short
  type JDouble = java.lang.Double
  type JBigDecimal = java.math.BigDecimal
  type JString = java.lang.String
  type JStringBuilder = java.lang.StringBuilder
  type JStringBuffer = java.lang.StringBuffer
  type JMap[K, V] = java.util.Map[K, V]
  type JHashMap[K, V] = java.util.HashMap[K, V]
  type JLinkedHashMap[K, V] = java.util.LinkedHashMap[K, V]
  type JConcurrentHashMap[K, V] = java.util.concurrent.ConcurrentHashMap[K, V]
  type JSet[E] = java.util.Set[E]
  type JHashSet[E] = java.util.HashSet[E]
  type JLinkedHashSet[E] = java.util.LinkedHashSet[E]
  type JList[E] = java.util.List[E]
  type JArrayList[E] = java.util.ArrayList[E]
  type JLinkedList[E] = java.util.LinkedList[E]
  type JQueue[E] = java.util.Queue[E]
  type JPriorityQueue[E] = java.util.PriorityQueue[E]
  type JCollections = java.util.Collections
}
