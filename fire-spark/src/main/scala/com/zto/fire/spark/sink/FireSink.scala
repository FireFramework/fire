package com.zto.fire.spark.sink

import com.zto.fire.spark.util.{SparkSingletonFactory, SparkUtils}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.Sink

/**
 * Fire框架组件sink父类
 *
 * @author ChengLong 2019年12月23日 10:09:55
 * @since 0.4.1
 */
abstract class FireSink extends Sink with Logging {
  @volatile protected var latestBatchId = -1L
  protected lazy val spark = SparkSingletonFactory.getSparkSession

  /**
   * 将内部row类型的DataFrame转为Row类型的DataFrame
   *
   * @param df
   * InternalRow类型的DataFrame
   * @return
   * Row类型的DataFrame
   */
  protected def toExternalRow(df: DataFrame): DataFrame = {
    SparkUtils.toExternalRow(df)
  }
}