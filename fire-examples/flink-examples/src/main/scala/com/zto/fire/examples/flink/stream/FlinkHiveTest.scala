package com.zto.fire.examples.flink.stream

import com.zto.fire._
import com.alibaba.fastjson.JSON
import com.zto.fire.examples.bean.Student
import com.zto.fire.flink.BaseFlinkStreaming
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
    // 调用startNewChain与setParallelism一样，都有会导致使用新的slotGroup，也都是作用于点之前的算子
    // startNewChain后，前面的那个算子会使用default的parallelism
    dstream.filter(s => s != null).startNewChain().map(s => {
      Thread.sleep(1000 * 60)
      s
    }).createOrReplaceTempView("kafka")
    this.flink.sql("select * from kafka").show
    // 查询操作
    this.flink.sql("select * from tmp.zto_scan_send order by bill_code limit 10").createOrReplaceTempView("scan_send")
    val joinedTable = this.flink.sql("select t1.bill_code, t2.name from scan_send t1 left join kafka t2 on t1.bill_code=t2.name")
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
