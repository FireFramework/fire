package com.zto.fire.flink.ext.core.batch

import com.zto.fire.common.util.ValueUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

import scala.reflect.ClassTag

/**
 * 用于flink ExecutionEnvironment API库扩展
 *
 * @author ChengLong 2020年1月9日 13:52:16
 * @since 0.4.1
 */
class BatchExecutionEnvExt(env: ExecutionEnvironment) {

  /**
   * 提交job执行
   *
   * @param jobName
   * job名称
   */
  def start(jobName: String = ""): Unit = {
    if (ValueUtils.isEmpty(jobName)) this.env.execute() else this.env.execute(jobName)
  }

  /**
   * 使用集合元素创建DataStream
   * @param seq
   *            元素集合
   * @tparam T
   *           元素的类型
   */
  def parallelize[T: TypeInformation: ClassTag](seq: Seq[T]): DataSet[T] = {
    this.env.fromCollection[T](seq)
  }
}
