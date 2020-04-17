package com.zto.fire.demo.flink.stream

import com.alibaba.fastjson.JSON
import com.zto.fire.common.anno.Scheduled
import com.zto.fire.demo.bean.Student
import com.zto.fire.flink.core.BaseFlinkStreaming
import com.zto.fire.flink.core.ext.FlinkExt._
import com.zto.fire.flink.core.util.FlinkUtils
import org.apache.flink.api.scala._


/**
 * flink 整合hive的例子，在流中join hive数据
 *
 * @author ChengLong 2020年4月3日 09:05:53
 */
object FlinkHiveTest extends BaseFlinkStreaming {

  override def process: Unit = {
    // 第三个参数需指定hive-site.xml具体的目录路径
    val dstream = this.ssc.createDirectStream().map(t => JSON.parseObject(t, classOf[Student]))
    dstream.createOrReplaceTempView("kafka")
    this.flink.sql("select * from kafka").show
    // 查询操作
    this.flink.sql("select * from tmp.zto_scan_send order by bill_code limit 10").createOrReplaceTempView("scan_send")
    val joinedTable = this.flink.sql("select t1.bill_code, t2.id, t2.name from scan_send t1 left join kafka t2 on t1.bill_code=t2.name")
    joinedTable.toRetractStream.print()

    this.ssc.startAwaitTermination()
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}
