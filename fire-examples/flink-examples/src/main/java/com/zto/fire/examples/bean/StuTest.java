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

package com.zto.fire.examples.bean;

import com.zto.fire.hbase.anno.HConfig;
import com.zto.fire.hbase.bean.HBaseBaseBean;

import java.util.*;

/**
 * 对应HBase表的JavaBean
 *
 * @author ChengLong 2019-6-20 16:06:16
 */
@HConfig(nullable = false, timestampField = "version")
public class StuTest extends HBaseBaseBean<StuTest> {
    protected Long id;
    protected String name;
    protected String number;
    protected Long version;

    /**
     * rowkey的构建
     *
     * @return
     */
    @Override
    public StuTest buildRowKey() {
        this.rowKey = this.id.toString();
        return this;
    }

    public StuTest() {
    }

    public StuTest(Long id, String name, String number, Long version) {
        this.id = id;
        this.name = name;
        this.number = number;
        this.version = version;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getNumber() {
        return number;
    }

    public void setNumber(String number) {
        this.number = number;
    }

    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public static List<StuTest> newStudentList() throws InterruptedException {
        List<StuTest> lists = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            for (int j = 1; j <= 10; j++) {
                lists.add(new StuTest((long) i, "admin1007", "1007", 1007L));
                Thread.sleep(10);
                // version比1007小，所有列均不会覆盖之前的记录
                lists.add(new StuTest((long) i, null, j + "", (long) j));
                Thread.sleep(10);
                // 比1007大，仅覆盖name列为admin1008，number列不写入
                lists.add(new StuTest((long) i, "admin1008", null, 1008L));
            }
        }

        return lists;
    }
}