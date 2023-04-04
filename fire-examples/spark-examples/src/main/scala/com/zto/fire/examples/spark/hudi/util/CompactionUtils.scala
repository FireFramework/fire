package com.zto.fire.examples.spark.hudi.util

import com.zto.fire._
import com.zto.fire.common.bean.TableIdentifier
import com.zto.fire.common.util.{FileUtils, HDFSUtils, IOUtils, JSONUtils}
import com.zto.fire.spark.sql.SparkSqlUtils

import java.io.File

/**
 * hudi表compaction工具
 *
 * @author ChengLong 2023-04-03 17:14:50
 * @since 2.3.5
 */
object CompactionUtils {
  // hudi元数据目录名称
  private val hoodieMetaDir = ".hoodie"
  // 本地存放schema文件的路径
  private val schemaLocalDir = "/Users/insight/Downloads/"

  /**
   * 解析并上传hudi表的schema文件到.hoodie目录下
   * 如果schema文件已存在，则
   *
   * @param hudiTableName
   * hudi表名（库名.表名）
   * @param tableLocal
   * hudi表所在的hdfs路径，为空则表示hive内部表
   * @param overwrite
   * 是否在schema文件已存在的情况下强制生成并覆盖
   */
  def schemaUpload(hudiTableName: String, tableLocal: String = "", overwrite: Boolean = false): Unit = {
    requireNonEmpty(hudiTableName)("hudi表名不能为空！")
    // 1.1 获取hudi表的绝对路径
    val tablePath = if (noEmpty(tableLocal)) tableLocal else SparkSqlUtils.getTablePath(hudiTableName)
    requireNonEmpty(tablePath)(s"hudi表路径为空，请确认hudi表${hudiTableName}是否已经创建，或通过参数传入")

    // 1.2 获取.hoodie目录的绝对路径
    val hudiMetaPath = this.hudiMetaPath(tablePath, this.hoodieMetaDir)
    requireNonEmpty(hudiMetaPath)("hudi表元数据目录绝对路径不能为空，请检查")

    // 1.3 判断该表的schema文件是否已存在
    val schemaFileIsExists = FileUtils.exists(hudiMetaPath)
    // 如果没有开启强制覆盖模式，并且文件已存在，则直接返回
    if (schemaFileIsExists && !overwrite) return

    // 2. 如果schema文件不存在，则上传schema文件到.hoodie路径下
    // 2.2 获取.hoodie路径下不为空的，保护表schema的文件路径
    val schemaJson = this.getHudiSchema(hudiMetaPath)
    requireNonEmpty(schemaJson)("hudi表schema json不能为空，请重试获取")

    // 2.3 将json数据刷到本地文件中，以表名开头，以.json为结尾
    val jsonFile = this.generateSchemaFile(hudiTableName, this.schemaLocalDir, schemaJson, overwrite)
    require(FileUtils.exists(jsonFile), s"schema文件不存在，请检查路径：$jsonFile")

    // 2.4 将上一步生成的json文件上传到hudiMetaPath路径下
    this.doUpload(jsonFile, hudiMetaPath, overwrite)
  }

  /**
   * 将指定文件上传到hdfs指定路径下
   *
   * @param file
   * json文件的
   * @param destPath
   * hdfs目标路径
   * @param overwrite
   * 是否覆盖已存在的文件
   */
  def doUpload(file: String, destPath: String, overwrite: Boolean): Unit = {
    HDFSUtils.upload(file, destPath, overwrite)
  }

  /**
   * 将json文件写入到本地路径下
   * 需要保障生成的文件包含完整的json schema字符串
   *
   * @param hudiTableName
   * hudi表名
   * @param localPath
   * 本地存放该json文件的路径
   * @param json
   * json schema字符串
   * @param overwrite
   * 是否强制重新生成
   * @return
   * 生成的本地json文件绝对路径
   */
  def generateSchemaFile(hudiTableName: String, localPath: String, json: String, overwrite: Boolean): String = {
    requireNonEmpty(hudiTableName, localPath, json)("生成hudi schema文件失败，部分参数为空")

    // 创建用于存放schema的目录
    val schemaDir = new File(localPath)
    if (!schemaDir.exists()) schemaDir.mkdir()

    // 如果文件已存在，并且开启强制覆盖模式，则删除旧文件重新生成新的schema文件
    val jsonFile = this.schemaFilePath(localPath, hudiTableName)
    IOUtils.writeText(jsonFile, json, overwrite)
    jsonFile
  }

  /**
   * 生成hudi表schema File的完整路径
   *
   * @param dir
   * 存放文件的目录路径
   * @param hudiTableName
   * hudi表名
   * @return
   * 完整路径
   */
  def schemaFilePath(dir: String, hudiTableName: String): String = {
    requireNonEmpty(dir, hudiTableName)("路径或hudi表名不能为空！")
    val parentDir = if (dir.endsWith("/")) dir.trim else dir.trim + "/"
    parentDir + TableIdentifier(hudiTableName).table
  }

  /**
   * 获取有效的，包含hudi表schema的文件绝对路径
   *
   * @param hudiMetaPath
   * hudi元数据目录绝对路径
   * @return
   * schema的json字符串
   */
  def getHudiSchema(hudiMetaPath: String): String = {
    // TODO: 根据timeline自动推导，暂时先写死
    val schemaFilePath = "hdfs://appcluster:8020/user/hive/warehouse/hudi.db/hudi_cj_pg_bill_item_newpg_cdc/.hoodie/20230321134221075.deltacommit"
    val json = HDFSUtils.readTextFile(schemaFilePath)
    requireNonEmpty(json)("获取的json为空，请检查")

    val schemaMap = JSONUtils.parseObject[JHashMap[String, Object]](json)
    require(schemaMap.containsKey("extraMetadata"), s"json中不包含extraMetadata，请检查：${schemaFilePath}")

    val extraMetadataMap = schemaMap.get("extraMetadata").asInstanceOf[JHashMap[String, Object]]
    require(extraMetadataMap.containsKey("schema"))
    extraMetadataMap.get("schema").toString
  }

  /**
   * 根据父目录获取hudi表的元数据（.hoodie目录）绝对路径
   *
   * @param parentPath
   * hudi表的path路径
   * @return
   * .hoodie目录的绝对路径
   */
  def hudiMetaPath(parentPath: String, hoodieMetaDir: String): String = {
    requireNonEmpty(parentPath, hoodieMetaDir)("hudi表父目录不能为空！")

    val appendSeparator = if (parentPath.endsWith("/")) parentPath else parentPath + "/"
    appendSeparator + hoodieMetaDir
  }

  /**
   * 用于判断指定hudi表当前是否可以执行异步compaction
   *
   * @param hudiTableName
   * hudi表名
   * @return
   */
  def canCompaction(hudiTableName: String): Boolean = {
    true
  }

  /**
   * 获取一个可用于执行compaction的timeline instance
   * 注：该方法会不断轮询重试，直到获取一个可用于执行compaction的instance
   *
   * @param hudiTableName
   * hudi表名
   * @return
   * 用于compaction的instance
   */
  def retryAndGetCompactionInstance(hudiTableName: String): String = {
    ""
  }

  def main(args: Array[JString]): Unit = {
    val schemaJson = getHudiSchema("")
    val jsonFile = this.generateSchemaFile("hudi.hudi_event_test", this.schemaLocalDir, schemaJson, true)
    println(jsonFile)
  }
}
