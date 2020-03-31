package com.zto.fire.demo.flink.batch

import com.zto.fire.common.util.SystemInfoUtils
import com.zto.fire.flink.core.BaseFlinkBatch
import org.apache.flink.table.catalog.hive.HiveCatalog
import com.zto.fire.flink.core.ext.FlinkExt._

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
    this.flink.sql("select * from tmp.zto_scan_send limit 10").printSchema()
  }

  def main(args: Array[String]): Unit = {
    this.init()
    this.stop
  }
}
