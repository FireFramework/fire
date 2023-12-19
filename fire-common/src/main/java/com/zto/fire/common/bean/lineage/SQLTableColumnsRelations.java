/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.zto.fire.common.bean.lineage;

import java.util.Objects;

/**
 * 用于封装采集到SQL的实时血缘信息：字段级血缘
 * @author wsczm
 */
public class SQLTableColumnsRelations {
    private String sourceColumn;
    private String targetColumn;

    public SQLTableColumnsRelations(String sourceColumn, String targetColumn) {
        this.sourceColumn = sourceColumn;
        this.targetColumn = targetColumn;
    }

    public String getSourceColumn() {
        return sourceColumn;
    }

    public void setSourceColumn(String sourceColumn) {
        this.sourceColumn = sourceColumn;
    }

    public String getTargetColumn() {
        return targetColumn;
    }


    public void setTargetColumn(String targetColumn) {
        this.targetColumn = targetColumn;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SQLTableColumnsRelations that = (SQLTableColumnsRelations) o;
        return Objects.equals(sourceColumn, that.sourceColumn) && Objects.equals(targetColumn, that.targetColumn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceColumn, targetColumn);
    }

}
