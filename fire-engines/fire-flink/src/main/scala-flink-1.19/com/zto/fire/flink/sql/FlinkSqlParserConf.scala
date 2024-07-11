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

package com.zto.fire.flink.sql

import org.apache.calcite.avatica.util.{Casing, Quoting}
import org.apache.calcite.sql.parser.{SqlParser => CalciteParser}
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl
import org.apache.flink.table.api.{SqlDialect => FlinkSqlDialect}

/**
 * Flink SQL解析器配置
 *
 * @author ChengLong
 * @Date 2024/7/11 16:27
 * @version 2.5.0
 */
object FlinkSqlParserConf extends FlinkSqlParserConfBase {

  /**
   * 构建flink default的SqlParser config
   */
  override def createParserConfig(dialect: FlinkSqlDialect = FlinkSqlDialect.DEFAULT): CalciteParser.Config = {
    val configBuilder = CalciteParser.configBuilder
      .setQuoting(Quoting.BACK_TICK)
      .setUnquotedCasing(Casing.TO_UPPER)
      .setQuotedCasing(Casing.UNCHANGED)

    configBuilder.setParserFactory(FlinkSqlParserImpl.FACTORY)
    configBuilder.build
  }
}
