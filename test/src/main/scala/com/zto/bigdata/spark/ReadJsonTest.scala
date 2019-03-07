package com.zto.bigdata.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * 读取json文件，并映射为json
  * 默认{"id": 1, "name": "root"}必须在同一行（只会解析第一行），代表一条记录
  * 如果有多条，则需要放到一个json array中，一个文件中多个json array只解析第一个
  */
object ReadJsonTest {
  val conf = new SparkConf()
    .setAppName("ReadJsonTest")
    .setMaster("local[*]")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config(conf).getOrCreate()
    // 指定mutiLine为true表示单个json允许换行
    val jsonDF = spark.read.option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ").option("multiLine", "true").json("J:\\test.json")
    jsonDF.printSchema()
    jsonDF.show()

    // 解析嵌套json中的值：
    /**
      * {
      * "id":1,
      * "name":"admin",
      * "list": {
      * "age": 12,
      * "sex": "男"
      * }
      * }
      */
    jsonDF.select("list.age").show()
    jsonDF.createOrReplaceTempView("json")
    spark.sql("select t.list.age from json t")

    spark.stop()
  }
}
