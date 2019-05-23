package com.zto.bigdata.spark.common.ext

import com.zto.bigdata.spark.common.bean.HBaseBaseBean
import com.zto.bigdata.spark.common.util.SingletonFactory
import org.apache.spark.streaming.dstream.DStream

import scala.reflect._

/**
  * DStream扩展
  *
  * @param stream
  * stream对象
  * @author ChengLong 2019-5-18 11:06:56
  */
class DStreamExt[T: ClassTag](stream: DStream[T]) {

  // 获取单例的HBaseContext对象
  private lazy val hbaseContext: HBaseContextExt = SingletonFactory.getHBaseContextInstance(stream.context.sparkContext)

  /**
    * DStrea数据实时写入
    *
    * @param tableName
    * HBase表名
    */
  def hbaseBulkPutStream[T <: HBaseBaseBean[T] : ClassTag](tableName: String, insertEmpty: Boolean = true, multiVersion: Boolean = false): Unit = {
    this.hbaseContext.bulkPutStream(tableName, stream.asInstanceOf[DStream[T]], insertEmpty, multiVersion)
  }

}