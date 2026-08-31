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

import com.zto.fire.common.anno.FieldName;
import com.zto.fire.common.bean.Generator;
import com.zto.fire.common.util.DateFormatUtils;
import com.zto.fire.common.util.JSONUtils;
import com.zto.fire.hbase.anno.HConfig;
import com.zto.fire.hbase.bean.HBaseBaseBean;

import java.io.Serializable;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;

/**
 * 对应HBase表的JavaBean，projection = true表示投影查询（仅查部分列）
 *
 * @author ChengLong 2019-6-20 16:06:16
 */
@HConfig(nullable = true, multiVersion = false, versions = 3, projection = true)
public class StudentProjection extends HBaseBaseBean<StudentProjection> implements Generator<StudentProjection>, Serializable {
    @FieldName(disuse = true)
    private static final long serialVersionUID = 1L;
    protected Long id;
    protected String name;
    @FieldName(disuse = true)
    protected Integer age;
    protected String createTime;

    /**
     * rowkey的构建
     *
     * @return
     */
    @Override
    public StudentProjection buildRowKey() {
        this.rowKey = this.id.toString();
        return this;
    }

    public StudentProjection(Long id, String name) {
        this.id = id;
        this.name = name;
    }

    public StudentProjection(Long id, String name, Integer age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    public StudentProjection(Long id, String name, Integer age, String createTime) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.createTime = createTime;
    }

    public StudentProjection() {

    }

    public StudentProjection(Long id) {
        this.id = id;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return JSONUtils.toJSONString(this);
    }

    @Override
    public StudentProjection generate() {
        Random random = new Random();
        this.id = Math.abs(random.nextLong());
        this.name = UUID.randomUUID().toString();
        this.age = Math.abs(random.nextInt(120));
        this.createTime = DateFormatUtils.formatCurrentDateTime();
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof StudentProjection)) {
            return false;
        }
        StudentProjection student = (StudentProjection) o;
        return Objects.equals(id, student.id) &&
                Objects.equals(name, student.name) &&
                Objects.equals(age, student.age) &&
                Objects.equals(createTime, student.createTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, age, createTime);
    }
}
