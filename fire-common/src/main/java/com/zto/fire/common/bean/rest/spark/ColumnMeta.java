package com.zto.fire.common.bean.rest.spark;

/**
 * 用于封装字段元数据
 *
 * @author ChengLong 2019-9-2 13:19:06
 */
public class ColumnMeta {
    // 所在数据库名称
    private String database;
    // 表名
    private String tableName;
    // 字段描述
    private String description;
    // 字段名
    private String columnName;
    // 字段类型
    private String dataType;
    // 是否允许为空
    private Boolean nullable;
    // 是否为分区字段
    private Boolean isPartition;
    // 是否为bucket字段
    private Boolean isBucket;

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public Boolean getNullable() {
        return nullable;
    }

    public void setNullable(Boolean nullable) {
        this.nullable = nullable;
    }

    public Boolean getPartition() {
        return isPartition;
    }

    public void setPartition(Boolean partition) {
        isPartition = partition;
    }

    public Boolean getBucket() {
        return isBucket;
    }

    public void setBucket(Boolean bucket) {
        isBucket = bucket;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public ColumnMeta() {
    }

    public ColumnMeta(String description, String database, String tableName, String columnName, String dataType, Boolean nullable, Boolean isPartition, Boolean isBucket) {
        this.description = description;
        this.database = database;
        this.tableName = tableName;
        this.columnName = columnName;
        this.dataType = dataType;
        this.nullable = nullable;
        this.isPartition = isPartition;
        this.isBucket = isBucket;
    }
}