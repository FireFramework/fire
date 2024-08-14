/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zto.fire.flink.ext.stream

import com.zto.fire._
import com.zto.fire.common.conf.FireHiveConf
import com.zto.fire.common.util.PropUtils
import com.zto.fire.flink.conf.FireFlinkConf
import com.zto.fire.flink.util.FlinkSingletonFactory
import com.zto.fire.noEmpty
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.api.{SqlDialect, Table, TableEnvironment}
import org.apache.flink.table.catalog.Catalog
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction, TableAggregateFunction, TableFunction}

import java.util.Optional

/**
 * 用于对Flink StreamTableEnvironment的API库扩展
 *
 * @author ChengLong 2020年1月7日 09:18:21
 * @since 0.4.1
 */
class TableEnvExt(tableEnv: TableEnvironment) extends TableApi {
  // 获取hive catalog
  lazy val hiveCatalog = this.getHiveCatalog
  // 获取paimon catalog
  lazy val paimonCatalog = this.getPaimonCatalog

  /**
   * 尝试获取注册的hive catalog对象
   */
  private def getHiveCatalog: Optional[Catalog] = this.getCatalog(FireHiveConf.hiveCatalogName)

  /**
   * 尝试获取注册的paimon catalog对象
   */
  private def getPaimonCatalog: Optional[Catalog] = this.getCatalog(PropUtils.getString("paimon.catalog.name", "paimon"))

  /**
   * 尝试根据名称获取catalog对象
   */
  private def getCatalog(catalogName: String): Optional[Catalog] = {
    if (isEmpty(catalogName)) return Optional.empty()

    val catalog = this.tableEnv.getCatalog(catalogName)
    if (catalog.isPresent) catalog else {
      val hiveCatalogName = this.tableEnv.listCatalogs().filter(_.contains(catalogName))
      if (noEmpty(hiveCatalogName)) this.tableEnv.getCatalog(hiveCatalogName(0)) else Optional.empty()
    }
  }
}

trait TableApi {
  private lazy val tableEnv = FlinkSingletonFactory.getTableEnv
  private lazy val streamTableEnv = FlinkSingletonFactory.getStreamTableEnv
  // 获取默认的catalog
  lazy val defaultCatalog = this.tableEnv.getCatalog(FireFlinkConf.defaultCatalogName)

  /**
   * 注册自定义udf函数
   *
   * @param name
   * 函数名
   * @param function
   * 函数的实例
   */
  def udf(name: String, function: ScalarFunction): Unit = {
    this.tableEnv.registerFunction(name, function)
  }

  /**
   * 用于判断当前是否hive catalog
   */
  def isHiveCatalog: Boolean = this.tableEnv.getCurrentCatalog.toUpperCase.contains("HIVE")

  /**
   * 用于判断当前是否为默认的catalog
   */
  def isDefaultCatalog: Boolean = !this.isHiveCatalog

  /**
   * 使用hive catalog
   */
  def useHiveCatalog(hiveCatalog: String = FireHiveConf.hiveCatalogName): Unit = {
    this.tableEnv.useCatalog(hiveCatalog)
    this.tableEnv.getConfig.setSqlDialect(SqlDialect.HIVE)
  }

  /**
   * 使用默认的catalog
   */
  def useDefaultCatalog: Unit = {
    this.tableEnv.useCatalog(FireFlinkConf.defaultCatalogName)
    this.tableEnv.getConfig.setSqlDialect(SqlDialect.DEFAULT)
  }

  /**
   * 获取当前catalog
   */
  def getCurrentCatalog: String = this.tableEnv.getCurrentCatalog

  /**
   * 创建临时视图
   */
  def createTemporaryView(path: String, view: Table): Unit = {
    this.tableEnv.createTemporaryView(path, view)
  }

  /**
   * 创建临时视图
   */
  def createTemporaryView[T](path: String, dataStream: DataStream[T]): Unit = {
    this.streamTableEnv.createTemporaryView[T](path, dataStream)
  }

  // ---------------------------- flink 内置api包装 ----------------------------------- //

  /**
   * Registers a [[TableFunction]] under a unique name in the TableEnvironment's catalog.
   * Registered functions can be referenced in SQL queries.
   *
   * @param name The name under which the function is registered.
   * @param tf The TableFunction to register
   *
   * @deprecated Use [[createTemporarySystemFunction(String, UserDefinedFunction)]] instead. Please
   *             note that the new method also uses the new type system and reflective extraction
   *             logic. It might be necessary to update the function implementation as well. See
   *             the documentation of [[TableFunction]] for more information on the new function
   *             design.
   */
  @deprecated
  def registerFunction[T: TypeInformation](name: String, tf: TableFunction[T]): Unit = {
    this.streamTableEnv.registerFunction[T](name, tf)
  }

  /**
   * Registers an [[AggregateFunction]] under a unique name in the TableEnvironment's catalog.
   * Registered functions can be referenced in Table API and SQL queries.
   *
   * @param name The name under which the function is registered.
   * @param f The AggregateFunction to register.
   * @tparam T The type of the output value.
   * @tparam ACC The type of aggregate accumulator.
   *
   * @deprecated Use [[createTemporarySystemFunction(String, UserDefinedFunction)]] instead. Please
   *             note that the new method also uses the new type system and reflective extraction
   *             logic. It might be necessary to update the function implementation as well. See
   *             the documentation of [[AggregateFunction]] for more information on the new
   *             function design.
   */
  @deprecated
  def registerFunction[T: TypeInformation, ACC: TypeInformation](
                                                                  name: String,
                                                                  f: AggregateFunction[T, ACC]): Unit = {
    this.streamTableEnv.registerFunction[T, ACC](name, f)
  }

  /**
   * Registers an [[TableAggregateFunction]] under a unique name in the TableEnvironment's catalog.
   * Registered functions can only be referenced in Table API.
   *
   * @param name The name under which the function is registered.
   * @param f The TableAggregateFunction to register.
   * @tparam T The type of the output value.
   * @tparam ACC The type of aggregate accumulator.
   *
   * @deprecated Use [[createTemporarySystemFunction(String, UserDefinedFunction)]] instead. Please
   *             note that the new method also uses the new type system and reflective extraction
   *             logic. It might be necessary to update the function implementation as well. See
   *             the documentation of [[TableAggregateFunction]] for more information on the new
   *             function design.
   */
  @deprecated
  def registerFunction[T: TypeInformation, ACC: TypeInformation](
                                                                  name: String,
                                                                  f: TableAggregateFunction[T, ACC]): Unit = {
    this.streamTableEnv.registerFunction[T, ACC](name, f)
  }

  /**
   * Converts the given [[DataStream]] into a [[Table]].
   *
   * The field names of the [[Table]] are automatically derived from the type of the
   * [[DataStream]].
   *
   * @param dataStream The [[DataStream]] to be converted.
   * @tparam T The type of the [[DataStream]].
   * @return The converted [[Table]].
   */
  def fromDataStream[T](dataStream: DataStream[T]): Table = {
    this.streamTableEnv.fromDataStream[T](dataStream)
  }

  /**
   * Converts the given [[DataStream]] into a [[Table]] with specified field names.
   *
   * There are two modes for mapping original fields to the fields of the [[Table]]:
   *
   * 1. Reference input fields by name:
   * All fields in the schema definition are referenced by name
   * (and possibly renamed using an alias (as). Moreover, we can define proctime and rowtime
   * attributes at arbitrary positions using arbitrary names (except those that exist in the
   * result schema). In this mode, fields can be reordered and projected out. This mode can
   * be used for any input type, including POJOs.
   *
   * Example:
   *
   * {{{
   *   val stream: DataStream[(String, Long)] = ...
   *   val table: Table = tableEnv.fromDataStream(
   *      stream,
   *      $"_2", // reorder and use the original field
   *      $"rowtime".rowtime, // extract the internally attached timestamp into an event-time
   *                          // attribute named 'rowtime'
   *      $"_1" as "name" // reorder and give the original field a better name
   *   )
   * }}}
   *
   * <p>2. Reference input fields by position:
   * In this mode, fields are simply renamed. Event-time attributes can
   * replace the field on their position in the input data (if it is of correct type) or be
   * appended at the end. Proctime attributes must be appended at the end. This mode can only be
   * used if the input type has a defined field order (tuple, case class, Row) and none of
   * the `fields` references a field of the input type.
   *
   * Example:
   *
   * {{{
   *   val stream: DataStream[(String, Long)] = ...
   *   val table: Table = tableEnv.fromDataStream(
   *      stream,
   *      $"a", // rename the first field to 'a'
   *      $"b" // rename the second field to 'b'
   *      $"rowtime".rowtime // extract the internally attached timestamp
   *                         // into an event-time attribute named 'rowtime'
   *   )
   * }}}
   *
   * @param dataStream The [[DataStream]] to be converted.
   * @param fields The fields expressions to map original fields of the DataStream to the fields of
   *               the [[Table]].
   * @tparam T The type of the [[DataStream]].
   * @return The converted [[Table]].
   */
  def fromDataStream[T](dataStream: DataStream[T], fields: Expression*): Table = {
    this.streamTableEnv.fromDataStream[T](dataStream, fields: _*)
  }

  /**
   * Creates a view from the given [[DataStream]].
   * Registered views can be referenced in SQL queries.
   *
   * The field names of the [[Table]] are automatically derived
   * from the type of the [[DataStream]].
   *
   * The view is registered in the namespace of the current catalog and database. To register the
   * view in a different catalog use [[createTemporaryView]].
   *
   * Temporary objects can shadow permanent ones. If a permanent object in a given path exists,
   * it will be inaccessible in the current session. To make the permanent object available again
   * you can drop the corresponding temporary object.
   *
   * @param name The name under which the [[DataStream]] is registered in the catalog.
   * @param dataStream The [[DataStream]] to register.
   * @tparam T The type of the [[DataStream]] to register.
   * @deprecated use [[createTemporaryView]]
   */
  @deprecated
  def registerDataStream[T](name: String, dataStream: DataStream[T]): Unit = {
    this.streamTableEnv.registerDataStream[T](name, dataStream)
  }

  /**
   * Creates a view from the given [[DataStream]] in a given path with specified field names.
   * Registered views can be referenced in SQL queries.
   *
   * There are two modes for mapping original fields to the fields of the View:
   *
   * 1. Reference input fields by name:
   * All fields in the schema definition are referenced by name
   * (and possibly renamed using an alias (as). Moreover, we can define proctime and rowtime
   * attributes at arbitrary positions using arbitrary names (except those that exist in the
   * result schema). In this mode, fields can be reordered and projected out. This mode can
   * be used for any input type, including POJOs.
   *
   * Example:
   *
   * {{{
   *   val stream: DataStream[(String, Long)] = ...
   *   tableEnv.registerDataStream(
   *      "myTable",
   *      stream,
   *      $"_2", // reorder and use the original field
   *      $"rowtime".rowtime, // extract the internally attached timestamp into an event-time
   *                          // attribute named 'rowtime'
   *      $"_1" as "name" // reorder and give the original field a better name
   *   )
   * }}}
   *
   * 2. Reference input fields by position:
   * In this mode, fields are simply renamed. Event-time attributes can
   * replace the field on their position in the input data (if it is of correct type) or be
   * appended at the end. Proctime attributes must be appended at the end. This mode can only be
   * used if the input type has a defined field order (tuple, case class, Row) and none of
   * the `fields` references a field of the input type.
   *
   * Example:
   *
   * {{{
   *   val stream: DataStream[(String, Long)] = ...
   *   tableEnv.registerDataStream(
   *      "myTable",
   *      stream,
   *      $"a", // rename the first field to 'a'
   *      $"b" // rename the second field to 'b'
   *      $"rowtime".rowtime // adds an event-time attribute named 'rowtime'
   *   )
   * }}}
   *
   * The view is registered in the namespace of the current catalog and database. To register the
   * view in a different catalog use [[createTemporaryView]].
   *
   * Temporary objects can shadow permanent ones. If a permanent object in a given path exists,
   * it will be inaccessible in the current session. To make the permanent object available again
   * you can drop the corresponding temporary object.
   *
   * @param name The name under which the [[DataStream]] is registered in the catalog.
   * @param dataStream The [[DataStream]] to register.
   * @param fields The fields expressions to map original fields of the DataStream to the fields of
   *               the View.
   * @tparam T The type of the [[DataStream]] to register.
   * @deprecated use [[createTemporaryView]]
   */
  @deprecated
  def registerDataStream[T](name: String, dataStream: DataStream[T], fields: Expression*): Unit = {
    this.streamTableEnv.registerDataStream[T](name, dataStream, fields: _*)
  }

  /**
   * Converts the given [[Table]] into an append [[DataStream]] of a specified type.
   *
   * The [[Table]] must only have insert (append) changes. If the [[Table]] is also modified
   * by update or delete changes, the conversion will fail.
   *
   * The fields of the [[Table]] are mapped to [[DataStream]] fields as follows:
   * - [[org.apache.flink.types.Row]] and Scala Tuple types: Fields are mapped by position, field
   * types must match.
   * - POJO [[DataStream]] types: Fields are mapped by field name, field types must match.
   *
   * @param table The [[Table]] to convert.
   * @tparam T The type of the resulting [[DataStream]].
   * @return The converted [[DataStream]].
   */
  def toAppendStream[T: TypeInformation](table: Table): DataStream[T] = {
    this.streamTableEnv.toAppendStream[T](table)
  }

  /**
   * Converts the given [[Table]] into a [[DataStream]] of add and retract messages.
   * The message will be encoded as [[Tuple2]]. The first field is a [[Boolean]] flag,
   * the second field holds the record of the specified type [[T]].
   *
   * A true [[Boolean]] flag indicates an add message, a false flag indicates a retract message.
   *
   * @param table The [[Table]] to convert.
   * @tparam T The type of the requested data type.
   * @return The converted [[DataStream]].
   */
  def toRetractStream[T: TypeInformation](table: Table): DataStream[(Boolean, T)] = {
    this.streamTableEnv.toRetractStream[T](table)
  }
}