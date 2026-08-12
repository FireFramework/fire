package com.zto.fire.examples.flink.lineage

import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.common.lineage.LineageManager
import com.zto.fire.core.anno.connector.{HBase, Jdbc}
import com.zto.fire.examples.bean.Student
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming

@Config(
  """
    |fire.lineage.enable=true
    |fire.lineage.api.enable=true
    |fire.lineage.debug.print=true
    |""")
@HBase("fat")
@Streaming(10)
@Jdbc(url = "jdbc:mysql://mysql-server:3306/fire?useSSL=true", username = "root", password = "root")
object ApiLineageTest extends FlinkStreaming {
  override def process(): Unit = {
    // 基于JDBC进行查询
    val students = this.fire.jdbcQueryList[Student]("select * from spark_test where age>=?", Seq(1))
    println("总计：" + students.length)

    // 基于HBase进行查询
    val rowKeys = Seq("1", "2", "3", "5", "6")
    val studentList = this.fire.hbaseGetListAsync2[Student]("fire_test_13", 1, rowKeys)
    studentList.foreach(println)

    val dstream = this.fire.createRandomLongStream(100)
    dstream.print()
    LineageManager.show(30)
  }
}
