package com.zto.fire.mq.deserializer

import com.zto.fire.flink.conf.ZtoSerializeConf
import com.zto.fire.mq.utils.DeserializeUtil
import com.zto.fire.predef._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.rocketmq.common.message.MessageExt
import org.apache.rocketmq.flink.common.serialization.TagKeyValueDeserializationSchema

import scala.reflect.ClassTag

/**
 * 自定义反序列化器，支持反序列化kafka消息中的key与value
 */
class ZtoRocketMessageDeserializationSchema[T <: BaseDeserializeEntity:ClassTag](topic: String)  extends TagKeyValueDeserializationSchema[T]  {

  @volatile private var initialized = false

  private def initOnce(): Unit = synchronized {
    if (!initialized) {
      DeserializeUtil.initZtoSerializer(topic)
      initialized = true
    }
  }

  override def deserializeTagKeyAndValue(msg: MessageExt): T = {
    try {
      this.initOnce()
      val headerVal = msg.getProperty(ZtoSerializeConf.SERIALIZER_HEAD_KEY)
      if (headerVal == null) {
        throw new RuntimeException("rocket 反序列化失败，未获取到消息头或初始化异常，请检查消息内容是否有误")
      }
      DeserializeUtil.deserializeAndFill(getGeneric[T](),headerVal,msg.getBody)
    } catch {
      case e: Throwable => {
        throw new RuntimeException("rocket 反序列化失败", e)
      }
    }
  }

  override def getProducedType: TypeInformation[T] = TypeInformation.of(getGeneric[T]())

}



