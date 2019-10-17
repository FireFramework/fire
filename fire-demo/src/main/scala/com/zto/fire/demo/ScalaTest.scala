package com.zto.fire.demo

import java.util
import java.util.Map

import com.alibaba.fastjson.JSON
import com.google.gson.{Gson, JsonArray, JsonParser}
import com.zto.fire.common.util.StringsUtils
import com.zto.fire.core.BaseSparkCore
import com.zto.fire.core.util.SparkUtils
import com.zto.fire.demo.bean.BinlogOrder
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Dataset, Encoders, Row}
import org.apache.spark.sql.catalyst.json.JSONOptions
import org.apache.spark.sql.execution.datasources.json.{JsonDataSource, TextInputJsonDataSource}
import org.apache.spark.sql.types.{LongType, StringType, StructType}

import scala.collection.mutable.ListBuffer
import scala.collection.{JavaConversions, mutable}
import scala.reflect.{ClassTag, classTag}

/**
 * 用于测试scala代码
 *
 * @author ChengLong 2019-9-4 13:39:16
 */
object ScalaTest extends BaseSparkCore {

  val json =
    """
      |{
      |  id: 1,
      |  BILL_CODE: "json"
      |}
      |{
      |  id: 2,
      |  BILL_CODE: "spark"
      |}
      |{
      |  id: 3,
      |  BILL_CODE: "scala"
      |}
      |""".stripMargin

  val jsonArr =
    """
      |[{id: 1, BILL_CODE: 'admin'},{id: 2, BILL_CODE: 'root'},{id: 3, BILL_CODE: 'spark'}]
      |""".stripMargin

  /**
   * Spark处理逻辑
   * 注：此方法会被自动调用，不需要在main中手动调用
   */
  override def process: Unit = {
    /*
    val jsonOptionClass = Class.forName("org.apache.spark.sql.catalyst.json.JSONOptions")
    val method = jsonOptionClass.getDeclaredConstructor(classOf[Map[String, String]], classOf[String], classOf[String])
    val map = Map[String, String]("timestampFormat" -> "yyyy/MM/dd HH:mm:ss ZZ")
    val options = method.newInstance(map, this.spark.sessionState.conf.sessionLocalTimeZone, this.spark.sessionState.conf.columnNameOfCorruptRecord)
    println(options.toString)

    val ds = this.spark.createDataset(Seq(json, json))(Encoders.STRING)
    val jsonClass = Class.forName("org.apache.spark.sql.execution.datasources.json.TextInputJsonDataSource")
    val methods = jsonClass.getMethod("inferFromDataset", classOf[Dataset[String]], jsonOptionClass)

    val schema = methods.invoke(null, ds, options.asInstanceOf[JSONOptions])
    schema.asInstanceOf[StructType].foreach(t => println(t.name))
    // TextInputJsonDataSource.inferFromDataset(ds, options.asInstanceOf[JSONOptions])*/
    /*val jsonDS = this.spark.createDataset(Seq(json))(Encoders.STRING)
    val schema = this.spark.read.json(jsonDS).schema*/
    /*val map = JSON.parseObject(json, classOf[util.HashMap[Object, Object]])
    val data = JavaConversions.mapAsScalaMap(map).toSeq
    val ds = this.spark.createDataset(data)(Encoders.bean(Map[_, _]))
    ds.show(10, false)
    ds.printSchema()*/
    val rdd = this.sc.parallelize(Seq(json))
    val ds = this.spark.createDataset(rdd)(Encoders.STRING)
    this.parseJson(ds, classOf[BinlogOrder]).show()

    val rdd2 = this.sc.parallelize(Seq(jsonArr))
    val ds2 = this.spark.createDataset(rdd2)(Encoders.STRING)
    this.parseJson(ds2, classOf[BinlogOrder]).show()
  }

  /**
   * 通用的消息解析
   *
   * @param ds
   * 待解析的消息集合
   * @param parse
   * 消息的处理方式
   * @tparam E
   * 当前消息的类型
   * @tparam T
   * 处理后的返回值类型
   * @return
   * 解析后的Dataset
   */
  def parseMsg[E: ClassTag, T: ClassTag](ds: Dataset[T], parse: (Seq[T]) => Seq[E], batch: Int = 1000): Dataset[E] = {
    val tClass = classTag[E].runtimeClass.asInstanceOf[Class[E]]
    ds.mapPartitions(it => {
      val list = ListBuffer[E]()
      val batchList = ListBuffer[T]()

      it.foreach(t => {
        batchList += t
        if (batchList.size >= batch) {
          val parseList = parse(batchList)
          if (parseList != null && parseList.size > 0) {
            list ++= parseList
          }
          batchList.clear()
        }
      })

      if (batchList.size > 0) {
        val parseList = parse(batchList)
        if (parseList != null && parseList.size > 0) {
          list ++= parseList
        }
        batchList.clear()
      }
      list.iterator
    })(Encoders.bean(tClass))
  }

  /**
   * 用于解析json消息
   *
   * @param ds
   * 待解析的json数据集
   * @param schema
   * 解析后的格式
   * @param batch
   * 用于指定一次解析多少条消息
   * @return
   * 解析json后的数据集
   */
  def parseJson[E: ClassTag, T <: String : ClassTag](ds: Dataset[T], schema: Class[E], batch: Int = 2): Dataset[E] = {
    def parse(jsons: Seq[String]): ListBuffer[E] = {
      val list = ListBuffer[E]()
      if (jsons != null && jsons.size > 0) {
        val header = jsons(0)
        // json array
        if (StringUtils.isNotBlank(header) && header.contains("[") && header.contains("]")) {
          jsons.foreach(jsonArray => {
            if (StringUtils.isNotBlank(jsonArray)) {
              println(jsonArray)
              list ++= JavaConversions.asScalaBuffer(JSON.parseArray(jsonArray, schema))
            }
          })
        } else {
          // json
          val jsonArray = new StringBuilder("[")
          jsons.foreach(json => {
            if (StringUtils.isNotBlank(json)) {
              jsonArray.append(json)
            }
          })
          jsonArray.append("]")
          list ++= JavaConversions.asScalaBuffer(JSON.parseArray(jsonArray.toString(), schema))
        }
      }
      list
    }

    this.parseMsg[E, T](ds, parse _, batch)
  }

  def main(args: Array[String]): Unit = {
    /*this.init()
    this.stop*/
    JavaConversions.asScalaBuffer(JSON.parseArray(jsonArr, classOf[BinlogOrder])).foreach(println)
  }

}
