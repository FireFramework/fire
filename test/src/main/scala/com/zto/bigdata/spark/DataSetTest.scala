package com.zto.bigdata.spark

import org.apache.spark.sql.SparkSession
import SparkSession._

object DataSetTest {

  case class Student(id: Int, name: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val ds = spark.createDataset(List(Student(1, "root"), Student(2, "spark")))
    ds.printSchema()
    ds.count()
  }
}
