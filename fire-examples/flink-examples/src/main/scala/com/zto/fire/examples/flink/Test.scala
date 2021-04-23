package com.zto.fire.examples.flink

import com.zto.fire._
import com.zto.fire.common.util.{JSONUtils, PropUtils, StringsUtils}
import com.zto.fire.examples.bean.Student
import com.zto.fire.flink.BaseFlinkStreaming
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.scala._

/**
 * Flink流式计算任务模板
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2021-01-18 17:24
 */
object Test extends BaseFlinkStreaming {

  override def process: Unit = {
    val sql =
      """
        |     CREATE
        |     table    MyUserTable (
        |  id BIGINT,
        |  name STRING,
        |  age INT,
        |  status BOOLEAN,
        |  PRIMARY KEY (id) NOT ENFORCED
        |)
        |  WITH
        |   (
        |   'connector' = 'jdbc111',
        |   'url' = 'jdbc111:mysql://localhost:3306/mydatabase',
        |   'table-name' = 'users111'
        |   )   ;
        |""".stripMargin
    this.fire.sql(sql, 2)
  }



  def main(args: Array[String]): Unit = {
    this.init()
  }
}
