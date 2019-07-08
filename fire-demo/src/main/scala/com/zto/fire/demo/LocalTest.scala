package com.zto.fire.demo

import java.util

import com.zto.fire.core.BaseSparkCore
import org.apache.spark.sql.{Dataset, Encoders}
import com.zto.fire.core.ext.SparkExt._
import com.zto.fire.demo.bean.OrderCommon

object LocalTest extends BaseSparkCore {

  val json =
    """
      |{"table":"ZTDH.ZTO_BILL_PROV_ELEC","op_type":"I","op_ts":"2019-06-26 09:34:26.246174","current_ts":"2019-06-26T17:34:28.265000","pos":"00000424500488722732","after":{"ORDER_CODE":"bc86ca474406429290cd94ac78d48992","BILL_CODE":"75158212128422","USE_SITE":"株洲荷塘四部","USE_SITE_ID":1026026,"PRO_DATE":"2019-06-26 17:34:23","PRO_SITE":null,"PRO_SITE_ID":null,"EMP_NAME":null,"EMP_CODE":null,"EMP_ID":0,"CUST_NAME":"歌谷魅女旗舰店","CUST_CODE":"3317121212","CUST_ID":3317121212,"RECORD_DATE":"2019-06-26 17:34:23","BL_ONLINE":1,"PRO_MAN":null,"PRO_MAN_ID":null,"BL_USE":0,"USE_DATE":null,"BL_LOCK":1,"PLATFORMID":1,"DES_SITE":null,"SPARE_FIELD1":null,"DES_SITE_ID":null,"REMARK":null,"SPARE_FIELD2":null}}
      |{"table":"ZTDH.ZTO_BILL_PROV_ELEC","op_type":"I","op_ts":"2019-06-27 06:26:58.182177","current_ts":"2019-06-27T14:27:02.859000","pos":"00000426070179988361","after":{"ORDER_CODE":"ZTO1906274193260203","BILL_CODE":"73115567967814","USE_SITE":"榆林靖边县","USE_SITE_ID":6200,"PRO_DATE":"2019-06-27 14:26:55","PRO_SITE":"上海","PRO_SITE_ID":2743,"EMP_NAME":null,"EMP_CODE":"91211.025","EMP_ID":340000,"CUST_NAME":"北郊王饼","CUST_CODE":"91211.025","CUST_ID":1003140023,"RECORD_DATE":"2019-06-27 14:26:55","BL_ONLINE":0,"PRO_MAN":"系统","PRO_MAN_ID":null,"BL_USE":0,"USE_DATE":"2019-06-27 14:26:55","BL_LOCK":2,"PLATFORMID":2,"DES_SITE":null,"SPARE_FIELD1":null,"DES_SITE_ID":null,"REMARK":null,"SPARE_FIELD2":null}}
    """.stripMargin

  /**
    * Spark处理逻辑
    * 注：此方法会被自动调用，不需要在main中手动调用
    */
  override def process: Unit = {
    this.spark.sql("set spark.sql.caseSensitive=true")
    val list = new util.ArrayList[String]()
    list.add(json)
    val jsonDS = this.spark.createDataset(list)(Encoders.STRING)
    this.spark.read.json(jsonDS).createOrReplaceTempView("test")
    this.spark.table("test").printSchema()
    this.spark.sql("select after.* from test").printSchema()
    this.spark.sql("select after.* from test").show(10, false)
    println("======================rdd1==========================")
    this.spark.sql("select after.* from test").toRDD(classOf[OrderCommon], true).collect().foreach(println)
    println("======================rdd2==========================")
    this.spark.sql("select after.* from test").toRDD(classOf[OrderCommon]).collect().foreach(println)
  }


  def main(args: Array[String]): Unit = {
    this.init()
    this.stop
  }
}