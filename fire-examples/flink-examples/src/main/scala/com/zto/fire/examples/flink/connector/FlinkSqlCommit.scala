package com.zto.fire.examples.flink.connector

import java.nio.file.{Files, Paths}

import com.zto.fire.common.util.PropUtils
import com.zto.fire.flink.BaseFlinkStreaming
import com.zto.fire.sql.SqlCommandParser
import org.apache.calcite.avatica.util.{Casing, Quoting}
import org.apache.calcite.sql.{SqlIdentifier, SqlNodeList}
import org.apache.calcite.sql.parser.SqlParser
import org.apache.flink.sql.parser.dml.RichSqlInsert
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl
import org.apache.flink.sql.parser.validate.FlinkSqlConformance
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, SqlDialect, SqlParserException}
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.hadoop.hive.conf.HiveConf

import scala.collection.JavaConversions._

object FlinkSqlCommit extends BaseFlinkStreaming {

  val HIVE_CATALOG_NAME = "hive_catalog"
  val HIVE_WAREHOUSE_DEFAULT_PATH = "hdfs:///user/hive/warehouse"
  var hiveMetaStoreUrl = ""
  var hiveVersion = ""

  var sqlFile: String = null
  var useHive: Boolean = false
  var hiveTableName: String = null

  def main(args: Array[String]): Unit = {

    println("参数：" + args.mkString(","))
    sqlFile = args(0)
    this.init()
  }

  /**
   * 生命周期方法：具体的用户开发的业务逻辑代码
   * 注：此方法会被自动调用，不需要在main中手动调用
   */
  override def process: Unit = {

    /**
     * run-cluster模式下需要手动开启checkpoint
     * run-application模式checkpoint可以通过配置开启
     */
    val settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build()
    env.enableCheckpointing(60000)
    this.tableEnv = StreamTableEnvironment.create(env,settings)

    hiveMetaStoreUrl = PropUtils.getString("flink.sql.submit.hive.metastore.url","thrift://SHTL009046107:9083")
    hiveVersion = PropUtils.getString("flink.sql.submit.hive.version","1.1.1")
    println("hiveMetaStoreUrl：" + hiveMetaStoreUrl)
    println("hiveVersion：" + hiveVersion)

    println("sql file path: " + sqlFile)


    /**
     *  run-application模式下，需要从hdfs复制到本地
     */
//    val localPath = "/tmp/" + sqlFile.substring(sqlFile.lastIndexOf("/") + 1)
//    logger.info("localPath:" + localPath)
//    SqlCommandParser.copyHdfsFileToLocal(localPath, localPath)
//    sqlFile = localPath

    val listSql = Files.readAllLines(Paths.get(sqlFile));
    logger.info("execute sql: \n" + listSql.mkString("\n"))
    parseInsertInto(listSql)

    val calls = SqlCommandParser.parse(listSql);
    for (call <- calls) {
      callCommand(call)
    }

  }

  /***
   *
   * @param listSql
   * 判断插入语句中是否有库名，如果有库名，则判断是Hive，并注册Catalog，可能会生成bug
   * 需要协调前端传入对应的sink引擎
   */
  private def parseInsertInto(listSql:java.util.List[String]): Unit ={

    val insertSql = "insert into"
    var hiveDatabaseName = "dw"
    for (sql <- listSql) {
      if(sql.toLowerCase.contains(insertSql)){
          val sqlNodeList: SqlNodeList = getSqlParse(sql).parseStmtList
          val sqlSelect: RichSqlInsert = sqlNodeList.getList.get(0).asInstanceOf[RichSqlInsert]
          val names =  sqlSelect.getTargetTable.asInstanceOf[SqlIdentifier].names
          if(names.size() > 1){
            useHive = true
            hiveDatabaseName = names.get(0).toLowerCase()
            hiveTableName = names.get(0) + "." + names.get(1)
            hiveTableName = hiveTableName.toLowerCase()
          }
      }
    }

    /**
     * 注册hive catalog
     */
    if(useHive) {
      val hiveConf = new HiveConf()
      hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, hiveMetaStoreUrl)
      hiveConf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, HIVE_WAREHOUSE_DEFAULT_PATH)
      val hiveCatalog = new HiveCatalog(HIVE_CATALOG_NAME, hiveDatabaseName, hiveConf, hiveVersion)
      tableEnv.registerCatalog(HIVE_CATALOG_NAME, hiveCatalog)
    }
  }

  /***
   * 根据sql语句，获取SqlParse
   * @param sql
   * @return
   */
   def getSqlParse(sql:String): SqlParser={
    val parser = SqlParser.create(sql,
      SqlParser.configBuilder.setParserFactory(
        FlinkSqlParserImpl.FACTORY).
        setQuoting(Quoting.BACK_TICK).
        setUnquotedCasing(Casing.TO_UPPER).
        setQuotedCasing(Casing.UNCHANGED).
        setConformance(FlinkSqlConformance.DEFAULT).
        build
    )
    parser
  }

  private def callCommand(cmdCall: SqlCommandParser.SqlCommandCall): Unit = {
    cmdCall.command match {
      case SqlCommandParser.SqlCommand.SET =>
        callSet(cmdCall)

      case SqlCommandParser.SqlCommand.CREATE_TABLE =>
        callCreateTable(cmdCall)

      case SqlCommandParser.SqlCommand.CREATE_VIEW =>
        callCreateTable(cmdCall)

      case SqlCommandParser.SqlCommand.INSERT_INTO =>
        callInsertInto(cmdCall)

      case _ =>
        throw new RuntimeException("Unsupported command: " + cmdCall.command)
    }
  }

  private def callSet(cmdCall: SqlCommandParser.SqlCommandCall): Unit = {
    val key = cmdCall.operands(0)
    val value = cmdCall.operands(1)
    this.tableEnv.executeSql("set " + key + "=" + value)
  }

  private def callCreateTable(cmdCall: SqlCommandParser.SqlCommandCall): Unit = {
    val ddl = cmdCall.operands(0)
    try {

      this.tableEnv.useCatalog("default_catalog")
      this.tableEnv.getConfig.setSqlDialect(SqlDialect.DEFAULT)

      this.tableEnv.executeSql(ddl)

    } catch {
      case e: SqlParserException =>
        throw new RuntimeException("SQL parse failed:\n" + ddl + "\n", e)
    }
  }

  private def callInsertInto(cmdCall: SqlCommandParser.SqlCommandCall): Unit = {
    var dml = cmdCall.operands(0)
    try {

      /**
       * 如果是Hive则需要切换对应的catalog,或者写catalog全路径：hive_catalog.dw.table_name
       */
      if (useHive) {
//        this.tableEnv.getConfig.setSqlDialect(SqlDialect.HIVE)
//        this.tableEnv.useCatalog(HIVE_CATALOG_NAME)
        dml = dml.replace(hiveTableName,HIVE_CATALOG_NAME + "." + hiveTableName);
      }

      //dml = dml.replace("from ", "from default_catalog.default_database.")
      println("dml:" + dml)
      this.tableEnv.executeSql(dml)
    } catch {
      case e: SqlParserException =>
        throw new RuntimeException("SQL parse failed:\n" + dml + "\n", e)
    }
  }

}
