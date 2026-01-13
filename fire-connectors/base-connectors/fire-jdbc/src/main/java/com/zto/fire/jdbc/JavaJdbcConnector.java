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

package com.zto.fire.jdbc;

import java.sql.Connection;

/**
 * Java版本的JDBC连接器，用于封装Scala的JdbcConnector
 *
 * @author chenglong 2025-11-27 14:32:52
 * @since 2.6.3
 */
public class JavaJdbcConnector {

    /**
     * 获取jdbc连接
     * @param keyNum
     * 对应配置文件的key值
     * @return
     * 数据库连接
     */
    public static Connection getConnection(int keyNum) {
        // 这里是获取数据库连接的具体实现，例如使用JDBC驱动
        return JdbcConnector$.MODULE$.getConnection(keyNum);
    }

    /**
     * 关闭指定的数据库连接
     */
    public static void closeConnection(Connection conn) {
        JdbcConnector$.MODULE$.closeConnection(conn);
    }
}
