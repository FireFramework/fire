package com.zto.fire.demo.flink.stream

import com.zto.fire.demo.bean.Student
import com.zto.fire.flink.core.BaseFlinkStreaming
import com.zto.fire.flink.core.ext.FlinkExt._
import org.apache.flink.api.scala._

/**
 * ogg消息解析测试
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-05-18 09:33
 */
object OGGTest extends BaseFlinkStreaming {

  override def process: Unit = {
    // 第三个参数需指定hive-site.xml具体的目录路径
    val dstream = this.ssc.createDirectStream().mapOgg(classOf[Student])
    dstream.map(t => {
      println(t.getTable + " " + t.getBefore.getId + " " + t.getAfter.getName)
      t
    }).print()

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
