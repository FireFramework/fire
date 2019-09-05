package com.zto.fire.common.alarm

import com.zto.fire.common.bean.BaseLogging
import com.zto.fire.common.util.DateFormatUtils
import com.zto.fire.common.util.alarm.{DingUtils, PhoneUtils}

/**
  * 告警模块
  *
  * @author ChengLong 2019-9-5 10:11:30
  */
object AlarmOper extends BaseLogging {
  private[this] val module = "alarm"

  /**
    * 告警的消息头
    *
    * @return
    * 消息头
    */
  private[fire] def withTitle(msg: String): String = "【Fire框架: " + DateFormatUtils.formatCurrentDateTime + "】" + msg

  /**
    * 钉钉告警
    *
    * @param dingId
    * dingding的用户id
    * @param msg
    * 告警内容
    */
  private[fire] def ding(dingId: String, msg: String): Unit = {
    val content = this.withTitle(msg)
    DingUtils.sendMsg(dingId, content)
    this.log(s"钉钉告警：id=$dingId msg=$content", this.module)
  }

  /**
    * 短信告警
    *
    * @param phone
    * 电话号码
    * @param msg
    * 告警内容
    */
  private[fire] def sms(phone: String, msg: String): Unit = {
    val content = this.withTitle(msg)
    PhoneUtils.sendSms(phone, content)
    this.log(s"短信告警：phone=$phone msg=$content", this.module)
  }

  /**
    * 语音告警
    *
    * @param phone
    * 电话号码
    * @param msg
    * 告警内容
    */
  private[fire] def voice(phone: String, msg: String): Unit = {
    val content = this.withTitle(msg)
    PhoneUtils.sendVoice(phone, content)
    this.log(s"语音告警：phone=$phone msg=$content", this.module)
  }
}
