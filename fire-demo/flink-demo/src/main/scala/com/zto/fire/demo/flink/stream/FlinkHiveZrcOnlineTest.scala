package com.zto.fire.demo.flink.stream

import com.alibaba.fastjson.JSON
import com.zto.fire.common.bean.ogg.OGGBaseBean
import com.zto.fire.flink.core.BaseFlinkStreaming
import com.zto.fire.flink.core.ext.FlinkExt._
import org.apache.flink.api.scala._


/**
 * flink 整合hive的例子，在流中join hive数据
 *
 * @author ChengLong 2020年4月3日 09:05:53
 */
object FlinkHiveZrcOnlineTest extends BaseFlinkStreaming {

  override def process: Unit = {
    // 第三个参数需指定hive-site.xml具体的目录路径
    val dstream = this.ssc.createDirectStream().map(t => JSON.parseObject(t, classOf[OGGBaseBean]))
    // 调用startNewChain与setParallelism一样，都有会导致使用新的slotGroup，也都是作用于点之前的算子
    // startNewChain后，前面的那个算子会使用default的parallelism
    dstream.filter(s => s != null).startNewChain().map(s => {
      Thread.sleep(1000)
      s
    }).createOrReplaceTempView("kafka")
    this.flink.sql("select * from kafka").show
    // 查询操作
    this.flink.sql("select * from dim.baseorganize limit 10").createOrReplaceTempView("hiveTable")
    val joinedTable = this.flink.sql("select t1.code, t2.op_type from hiveTable t1 left join kafka t2 on t1.code=t2.op_type")
    joinedTable.toRetractStream.print()

    this.ssc.startAwaitTermination()
  }

  override def before(args: Array[String]): Unit = {
    if (args != null) {
      args.foreach(x => println("main方法参数：" + x))
    }
  }

  def main(args: Array[String]): Unit = {
    this.init(args = args)
  }
}
