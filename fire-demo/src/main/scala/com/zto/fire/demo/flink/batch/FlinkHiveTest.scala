package com.zto.fire.demo.flink.batch

import com.zto.fire.demo.bean.Student
import com.zto.fire.flink.core.{BaseFlinkBatch, BaseFlinkStreaming}
import com.zto.fire.flink.core.ext.FlinkExt._
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

/**
 * flink 整合hive的例子
 * @author ChengLong 2020年2月17日 13:35:50
 */
object FlinkHiveTest extends BaseFlinkBatch {

  override def process: Unit = {
    // 第三个参数需指定hive-site.xml具体的目录路径
    // val hive = new HiveCatalog("FlinkHiveTest", null, "J:\\Desktop\\实时平台\\flink", "1.2.1")
    /*val hive = new HiveCatalog("test", null, if (SystemInfoUtils.isWindows) "J:\\Desktop\\实时平台\\flink" else null, "1.2.1")
    this.tableEnv.registerCatalog("test", hive)
    this.tableEnv.useCatalog("test")*/

    // 查询操作
    // this.flink.sql("select * from tmp.flink_hive_test").printSchema()
    /*val table = this.flink.sql("select * from tmp.zto_scan_send order by bill_code limit 10")
    table.toDataSet.print()*/
    val table = this.sc.fromElements(Student.newStudentList()).toTable
    table.toDataSet[Student].print()
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}
