package com.zto.fire.spark.ext.provider

import com.zto.fire.core.ext.Provider
import com.zto.fire.spark.util.SparkSingletonFactory

/**
 * spark provider父接口
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2020-12-23 17:49
 */
trait SparkProvider extends Provider {
  protected lazy val spark = SparkSingletonFactory.sparkSession
  protected lazy val sc = spark.sparkContext
}
