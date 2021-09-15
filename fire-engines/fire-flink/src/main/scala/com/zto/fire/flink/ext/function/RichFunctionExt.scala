package com.zto.fire.flink.ext.function

import com.zto.fire._
import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction, RichFunction}
import org.apache.flink.api.common.state.{AggregatingState, AggregatingStateDescriptor, ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor, State, StateTtlConfig, ValueState, ValueStateDescriptor}

import scala.reflect.ClassTag

/**
 * RichFunction api扩展，支持方便的获取状态数据
 *
 * @author ChengLong 2021-9-14 09:59:17
 * @since 2.2.0
 */
class RichFunctionExt(richFunction: RichFunction) {
  lazy val runtimeContext = richFunction.getRuntimeContext
  private[this] lazy val stateMap = new JConcurrentHashMap[String, State]()


  /**
   * 根据name获取ValueState
   */
  def getState[T: ClassTag](name: String, ttlConfig: StateTtlConfig = null): ValueState[T] = {
    this.stateMap.mergeGet(name) {
      val desc = new ValueStateDescriptor[T](name, getParamType[T])
      if (ttlConfig != null) desc.enableTimeToLive(ttlConfig)
      this.runtimeContext.getState[T](desc)
    }.asInstanceOf[ValueState[T]]
  }

  /**
   * 根据name获取ListState
   */
  def getListState[T: ClassTag](name: String, ttlConfig: StateTtlConfig = null): ListState[T] = {
    this.stateMap.mergeGet(name) {
      val desc = new ListStateDescriptor[T](name, getParamType[T])
      if (ttlConfig != null) desc.enableTimeToLive(ttlConfig)
      this.runtimeContext.getListState[T](desc)
    }.asInstanceOf[ListState[T]]
  }

  /**
   * 根据name获取MapState
   */
  def getMapState[K: ClassTag, V: ClassTag](name: String, ttlConfig: StateTtlConfig = null): MapState[K, V] = {
    this.stateMap.mergeGet(name) {
      val desc = new MapStateDescriptor[K, V](name, getParamType[K], getParamType[V])
      if (ttlConfig != null) desc.enableTimeToLive(ttlConfig)
      this.runtimeContext.getMapState[K, V](desc)
    }.asInstanceOf[MapState[K, V]]
  }

  /**
   * 根据name获取ReducingState
   */
  def getReducingState[T: ClassTag](name: String, reduceFun: (T, T) => T, ttlConfig: StateTtlConfig = null): ReducingState[T] = {
    this.stateMap.mergeGet(name) {
      val desc = new ReducingStateDescriptor[T](name, new ReduceFunction[T] {
        override def reduce(value1: T, value2: T): T = reduceFun(value1, value2)
      }, getParamType[T])
      if (ttlConfig != null) desc.enableTimeToLive(ttlConfig)
      this.runtimeContext.getReducingState[T](desc)
    }.asInstanceOf[ReducingState[T]]
  }

  /**
   * 根据name获取AggregatingState
   */
  def getAggregatingState[I, T: ClassTag, O](name: String, aggFunction: AggregateFunction[I, T, O], ttlConfig: StateTtlConfig = null): AggregatingState[I, O] = {
    this.stateMap.mergeGet(name) {
      val desc = new AggregatingStateDescriptor[I, T, O](name, aggFunction, getParamType[T])
      if (ttlConfig != null) desc.enableTimeToLive(ttlConfig)
      this.runtimeContext.getAggregatingState(desc)
    }.asInstanceOf[AggregatingState[I, O]]
  }
}
