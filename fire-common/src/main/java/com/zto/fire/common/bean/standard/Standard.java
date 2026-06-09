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

package com.zto.fire.common.bean.standard;

import com.zto.fire.common.bean.FireTask;

import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * 代码规范检测 Kafka 消息体：FireTask 公共字段 + 去重后的检测结果列表。
 * 仅在 Driver/JobManager 发送 Kafka 前组装；Worker 端仅上报 {@link StandardResult}。
 *
 * @author ChengLong
 * @since 3.0.0
 */
public class Standard extends FireTask implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * 去重后的代码规范检测结果
     */
    private Set<StandardResult> results = new LinkedHashSet<>();

    public Standard() {
        super();
    }

    /**
     * 在 master 端将已汇总的检测结果包装为 Kafka 消息体
     */
    public static Standard wrapResults(Collection<StandardResult> results) {
        Standard standard = new Standard();
        if (results != null && !results.isEmpty()) {
            standard.results = new LinkedHashSet<>(results);
        }

        return standard;
    }

    public Set<StandardResult> getResults() {
        return results;
    }

    public void setResults(Set<StandardResult> results) {
        this.results = results == null ? new LinkedHashSet<>() : new LinkedHashSet<>(results);
    }
}
