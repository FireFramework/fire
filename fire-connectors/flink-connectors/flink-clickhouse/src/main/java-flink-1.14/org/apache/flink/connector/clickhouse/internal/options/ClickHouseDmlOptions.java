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

package org.apache.flink.connector.clickhouse.internal.options;

import javax.annotation.Nullable;

import java.time.Duration;

/** ClickHouse data modify language options. */
public class ClickHouseDmlOptions extends ClickHouseConnectionOptions {

    private static final long serialVersionUID = 1L;

    private final int batchSize;

    private final Duration flushInterval;

    private final int maxRetries;

    private final boolean useLocal;

    private final String partitionStrategy;

    private final String partitionKey;

    private final boolean ignoreDelete;

    private ClickHouseDmlOptions(
            String url,
            @Nullable String username,
            @Nullable String password,
            String databaseName,
            String tableName,
            int batchSize,
            Duration flushInterval,
            int maxRetires,
            boolean useLocal,
            String partitionStrategy,
            String partitionKey,
            boolean ignoreDelete) {
        super(url, username, password, databaseName, tableName);
        this.batchSize = batchSize;
        this.flushInterval = flushInterval;
        this.maxRetries = maxRetires;
        this.useLocal = useLocal;
        this.partitionStrategy = partitionStrategy;
        this.partitionKey = partitionKey;
        this.ignoreDelete = ignoreDelete;
    }

    public int getBatchSize() {
        return this.batchSize;
    }

    public Duration getFlushInterval() {
        return this.flushInterval;
    }

    public int getMaxRetries() {
        return this.maxRetries;
    }

    public boolean isUseLocal() {
        return this.useLocal;
    }

    public String getPartitionStrategy() {
        return this.partitionStrategy;
    }

    public String getPartitionKey() {
        return this.partitionKey;
    }

    public boolean getIgnoreDelete() {
        return this.ignoreDelete;
    }

    /** Builder for {@link ClickHouseDmlOptions}. */
    public static class Builder {
        private String url;
        private String username;
        private String password;
        private String databaseName;
        private String tableName;
        private int batchSize;
        private Duration flushInterval;
        private int maxRetries;
        private boolean writeLocal;
        private boolean useLocal;
        private String partitionStrategy;
        private String partitionKey;
        private boolean ignoreDelete;

        public Builder() {}

        public ClickHouseDmlOptions.Builder withUrl(String url) {
            this.url = url;
            return this;
        }

        public ClickHouseDmlOptions.Builder withUsername(String username) {
            this.username = username;
            return this;
        }

        public ClickHouseDmlOptions.Builder withPassword(String password) {
            this.password = password;
            return this;
        }

        public ClickHouseDmlOptions.Builder withDatabaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public ClickHouseDmlOptions.Builder withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public ClickHouseDmlOptions.Builder withBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public ClickHouseDmlOptions.Builder withFlushInterval(Duration flushInterval) {
            this.flushInterval = flushInterval;
            return this;
        }

        public ClickHouseDmlOptions.Builder withMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public ClickHouseDmlOptions.Builder withWriteLocal(Boolean writeLocal) {
            this.writeLocal = writeLocal;
            return this;
        }

        public ClickHouseDmlOptions.Builder withUseLocal(Boolean useLocal) {
            this.useLocal = useLocal;
            return this;
        }

        public ClickHouseDmlOptions.Builder withPartitionStrategy(String partitionStrategy) {
            this.partitionStrategy = partitionStrategy;
            return this;
        }

        public ClickHouseDmlOptions.Builder withPartitionKey(String partitionKey) {
            this.partitionKey = partitionKey;
            return this;
        }

        public ClickHouseDmlOptions.Builder withIgnoreDelete(boolean ignoreDelete) {
            this.ignoreDelete = ignoreDelete;
            return this;
        }

        public ClickHouseDmlOptions build() {
            return new ClickHouseDmlOptions(
                    url,
                    username,
                    password,
                    databaseName,
                    tableName,
                    batchSize,
                    flushInterval,
                    maxRetries,
                    writeLocal || useLocal,
                    partitionStrategy,
                    partitionKey,
                    ignoreDelete);
        }
    }
}
