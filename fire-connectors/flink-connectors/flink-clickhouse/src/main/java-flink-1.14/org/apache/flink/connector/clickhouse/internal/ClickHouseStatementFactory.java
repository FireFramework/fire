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

package org.apache.flink.connector.clickhouse.internal;

import org.apache.commons.lang3.ArrayUtils;

import java.util.Arrays;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

/** Create an insert/update/delete ClickHouse statement. */
public class ClickHouseStatementFactory {

    private static final String EMPTY = "";

    private ClickHouseStatementFactory() {}

    public static String getSelectStatement(
            String tableName, String databaseName, String[] fieldNames) {
        String columns =
                Arrays.stream(fieldNames)
                        .map(ClickHouseStatementFactory::quoteIdentifier)
                        .collect(joining(", "));
        return String.join(
                EMPTY, "SELECT ", columns, " FROM ", fromTableClause(tableName, databaseName));
    }

    public static String getInsertIntoStatement(String tableName, String[] fieldNames) {
        String columns =
                Arrays.stream(fieldNames)
                        .map(ClickHouseStatementFactory::quoteIdentifier)
                        .collect(joining(", "));
        String placeholders = Arrays.stream(fieldNames).map((f) -> "?").collect(joining(", "));
        return String.join(
                EMPTY,
                "INSERT INTO ",
                quoteIdentifier(tableName),
                "(",
                columns,
                ") VALUES (",
                placeholders,
                ")");
    }

    public static String getUpdateStatement(
            String tableName,
            String databaseName,
            String clusterName,
            String[] fieldNames,
            String[] keyFields,
            String[] partitionFields) {
        String setClause =
                Arrays.stream(fieldNames)
                        .filter(f -> !ArrayUtils.contains(keyFields, f))
                        .filter(f -> !ArrayUtils.contains(partitionFields, f))
                        .map((f) -> quoteIdentifier(f) + "=?")
                        .collect(joining(", "));
        String conditionClause =
                Arrays.stream(keyFields)
                        .map((f) -> quoteIdentifier(f) + "=?")
                        .collect(joining(" AND "));
        String onClusterClause = "";
        if (clusterName != null) {
            onClusterClause = " ON CLUSTER " + quoteIdentifier(clusterName);
        }

        return String.join(
                EMPTY,
                "ALTER TABLE ",
                fromTableClause(tableName, databaseName),
                onClusterClause,
                " UPDATE ",
                setClause,
                " WHERE ",
                conditionClause);
    }

    public static String getDeleteStatement(
            String tableName, String databaseName, String clusterName, String[] conditionFields) {
        String conditionClause =
                Arrays.stream(conditionFields)
                        .map((f) -> quoteIdentifier(f) + "=?")
                        .collect(joining(" AND "));
        String onClusterClause = "";
        if (clusterName != null) {
            onClusterClause = " ON CLUSTER " + quoteIdentifier(clusterName);
        }

        return String.join(
                EMPTY,
                "ALTER TABLE ",
                fromTableClause(tableName, databaseName),
                onClusterClause,
                " DELETE WHERE ",
                conditionClause);
    }

    private static String fromTableClause(String tableName, String databaseName) {
        if (databaseName == null) {
            return quoteIdentifier(tableName);
        }

        return format("%s.%s", quoteIdentifier(databaseName), quoteIdentifier(tableName));
    }

    private static String quoteIdentifier(String identifier) {
        return String.join(EMPTY, "`", identifier, "`");
    }
}
