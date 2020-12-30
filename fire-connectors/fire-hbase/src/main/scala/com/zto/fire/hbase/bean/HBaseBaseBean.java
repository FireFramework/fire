package com.zto.fire.hbase.bean;

import com.zto.fire.common.anno.FieldName;

import java.io.Serializable;

/**
 * HBase封装bean需实现该接口
 * Created by ChengLong on 2017-03-27.
 */
public abstract class HBaseBaseBean<T> implements Serializable {
    /**
     * rowKey字段
     */
    @FieldName(value = "rowKey", disuse = true)
    public String rowKey;

    /**
     * 子类包名+类名
     */
    @FieldName(value = "className", disuse = true)
    public final String className = this.getClass().getSimpleName();

    /**
     * 根据业务需要，构建rowkey
     */
    public abstract T buildRowKey();

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }
}
