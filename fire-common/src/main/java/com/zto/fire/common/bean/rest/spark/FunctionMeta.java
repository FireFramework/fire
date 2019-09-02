package com.zto.fire.common.bean.rest.spark;

/**
 * 用于封装函数元数据信息
 * @author ChengLong 2019-9-2 16:50:50
 */
public class FunctionMeta {
    // 函数描述
    private String description;
    // 数据库
    private String database;
    // 函数名称
    private String name;
    // 函数定义的类
    private String className;
    // 是否为临时函数
    private Boolean isTemporary;

    public FunctionMeta() {
    }

    public FunctionMeta(String description, String database, String name, String className, Boolean isTemporary) {
        this.description = description;
        this.database = database;
        this.name = name;
        this.className = className;
        this.isTemporary = isTemporary;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public Boolean getTemporary() {
        return isTemporary;
    }

    public void setTemporary(Boolean temporary) {
        isTemporary = temporary;
    }
}
