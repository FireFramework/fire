package com.zto.bigdata.spark.common.ext

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.parser.ParserConfig
import com.zto.bigdata.spark.common.bean.{HBaseBaseBean, OGGBaseBean}
import com.zto.bigdata.spark.common.util.SingletonFactory
import org.apache.commons.lang3.StringUtils
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ListBuffer
import scala.reflect._

/**
  * DStream扩展
  *
  * @param stream
  * stream对象
  * @author ChengLong 2019-5-18 11:06:56
  */
class DStreamExt(stream: DStream[(String, String)]) {

  // 获取单例的HBaseContext对象
  private lazy val hbaseContext: HBaseContextExt = SingletonFactory.getHBaseContextInstance(stream.context.sparkContext)

  /**
    * DStrea数据实时写入
    *
    * @param tableName
    * HBase表名
    */
  def streamBulkPut[T <: HBaseBaseBean[T] : ClassTag](tableName: String, insertEmpty: Boolean = true): Unit = {
    this.hbaseContext.streamBulkPut(tableName, stream.asInstanceOf[DStream[T]], insertEmpty)
  }

  /**
    * 将kafka过来的json格式数据映射为目标格式DStream
    *
    * @param oggBeanType
    * 对应json消息格式的JavaBean类型
    * @param targetBeanType
    * 目标类型
    * @return
    * 映射后的DStream
    */
  def parseJsonDStream[T <: OGGBaseBean : ClassTag, E <: HBaseBaseBean[E] : ClassTag](oggBeanType: Class[T], targetBeanType: Class[E]): DStream[E] = {
    stream.mapPartitions(it => {
      val oggClazz = classTag[T].runtimeClass
      val targetClazz = classTag[E].runtimeClass
      val getAfterMethod = oggClazz.getMethod("getAfter")
      val buildRowKeyMethod = targetClazz.getMethod("buildRowKey")
      val list = ListBuffer[E]()
      ParserConfig.getGlobalInstance.setAsmEnable(false)
      it.foreach(t => {
        if (StringUtils.isNotBlank(t._2)) {
          try {
            val jsonStr = t._2.trim
            if (jsonStr.startsWith("[") && jsonStr.endsWith("]")) {
              val oggBeanList = JSON.parseArray(jsonStr, oggClazz)
              if (oggBeanList != null && oggBeanList.size() > 0) {
                val it = oggBeanList.iterator()
                while (it.hasNext) {
                  val oggBean = it.next()
                  if (oggBean != null) {
                    val after = getAfterMethod.invoke(oggBean)
                    if (after != null) {
                      list += buildRowKeyMethod.invoke(after).asInstanceOf[E]
                    }
                  }
                }
              }
            } else {
              val oggBean = JSON.parseObject(jsonStr, oggClazz)
              if (oggBean != null) {
                val after = getAfterMethod.invoke(oggBean)
                if (after != null) {
                  list += buildRowKeyMethod.invoke(after).asInstanceOf[E]
                }
              }
            }
          } catch {
            case e: Exception => println(t._2)
          }
        }
      })
      list.iterator
    })
  }
}