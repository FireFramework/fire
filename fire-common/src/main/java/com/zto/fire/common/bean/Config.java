package com.zto.fire.common.bean;

/**
 * 配置bean，用于接收和解析main函数传递的json数据
 * @author ChengLong 2019-4-25 20:10:26
 */
public class Config {
    // 配置文件路径
    private String properties;

    public String getProperties() {
        return properties;
    }

    public void setProperties(String properties) {
        this.properties = properties;
    }
}
