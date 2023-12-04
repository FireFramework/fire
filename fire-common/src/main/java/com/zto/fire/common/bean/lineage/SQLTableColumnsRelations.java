package com.zto.fire.common.bean.lineage;

import java.util.Objects;

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
