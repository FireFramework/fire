package com.zto.fire.flink.ext.stream

import com.zto.fire.common.util.PropUtils
import com.zto.fire.flink.conf.FireFlinkConf
import com.zto.fire.{noEmpty, requireNonEmpty}
import org.slf4j.LoggerFactory

/**
 * Flink SQL扩展类
 *
 * @author ChengLong 2021-4-23 10:36:49
 * @since 2.0.0
 */
class SQLExt[K, V](sql: String) {
  // 用于Flink SQL中的with表达式
  private[this] lazy val withPattern = """(with|WITH)\s*\(([\s\S]*)""".r
  private lazy val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * 将给定的不包含with表达式的Flink SQL添加with表达式
   *
   * @param keyNum
   * with表达式在配置文件中声明的keyNum，小于零时，则表示不拼接with表达式
   * @return
   * 组装了with表达式的Flink SQL文本
   */
  def with$(keyNum: Int = 1): String = {
    requireNonEmpty(sql, "sql语句不能为空！")
    if (keyNum < 1) return sql
    val matcher = withPattern.findFirstIn(sql)

    // 如果SQL中已有with表达式，并且未开启with替换功能，则直接返回传入的sql
    if (matcher.isDefined && !FireFlinkConf.sqlWithReplaceModeEnable) {
      logger.warn(s"sql中已经包含with表达式，请移除后再使用动态with替换功能：\n${matcher.get}")
      if (FireFlinkConf.sqlLogEnable) logger.info(s"完整SQL语句：$sql")
      return sql
    }

    val withMap = PropUtils.sliceKeysByNum(FireFlinkConf.FLINK_SQL_WITH_PREFIX, keyNum)
    if (withMap.isEmpty) throw new IllegalArgumentException(s"配置文件中未找到以${FireFlinkConf.FLINK_SQL_WITH_PREFIX}开头以${keyNum}结尾的配置信息！")

    // 如果开启with表达式强制替换功能，则将sql中with表达式移除
    val fixSql = if (matcher.isDefined && FireFlinkConf.sqlWithReplaceModeEnable) withPattern.replaceAllIn(sql, "") else sql
    val finalSQL = buildWith(fixSql, withMap)
    if (FireFlinkConf.sqlLogEnable) logger.info(s"完整SQL语句：$finalSQL")
    finalSQL
  }

  /**
   * 根据给定的配置列表构建Flink SQL with表达式
   *
   * @param map
   * Flink SQL with配置列表
   * @return
   * with sql表达式
   */
  private[fire] def buildWith(sql: String, map: Map[String, String]): String = {
    val withSql = new StringBuilder()
    withSql.append(sql).append("WITH (\n")
    map.filter(conf => noEmpty(conf, conf._1, conf._2)).foreach(conf => {
      withSql
        .append(s"""\t'${conf._1}'=""")
        .append(s"'${conf._2}'")
        .append(",\n")
    })
    withSql.substring(0, withSql.length - 2) + "\n)"
  }
}