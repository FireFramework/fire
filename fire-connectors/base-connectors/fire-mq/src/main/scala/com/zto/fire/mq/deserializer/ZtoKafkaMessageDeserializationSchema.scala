package com.zto.fire.mq.deserializer

import com.zto.bigdata.integeration.util.ZtoSerializeUtil
import com.zto.fire.common.util.ReflectionUtils
import com.zto.fire.flink.conf.ZtoSerializeConf
import com.zto.fire.mq.utils.DeserializeUtil
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

import java.nio.charset.StandardCharsets
import scala.reflect.ClassTag
import com.zto.fire.predef._

/**
 * 自定义反序列化器，支持反序列化kafka消息中的key与value
 */
class ZtoKafkaMessageDeserializationSchema[T <: BaseDeserializeEntity:ClassTag](topic: String) extends KafkaDeserializationSchema[T] {

  override def isEndOfStream(t: T): Boolean = false

  override def open(context: DeserializationSchema.InitializationContext): Unit = {
    DeserializeUtil.initZtoSerializer(topic)
  }

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): T = {
    try{
      val headerVal = record.headers().lastHeader(ZtoSerializeConf.SERIALIZER_HEAD_KEY);
      if (headerVal == null) {
        throw new RuntimeException("Kafka 反序列化失败，未获取到消息头或初始化异常，请检查消息内容是否有误")
      }
      val headType = new String(headerVal.value(), StandardCharsets.UTF_8)
      DeserializeUtil.deserializeAndFill(getGeneric[T](),headType,record.value())
    } catch {
      case e: Throwable => {
        throw new RuntimeException("Kafka 反序列化失败", e)
      }
    }

  }

  override def getProducedType: TypeInformation[T] = TypeInformation.of(getGeneric[T]())
}


