package com.zto.fire.common.bean.ogg;

import java.io.Serializable;

/**
 * ogg基础字段包装类
 *
 * @author ChengLong
 * @create: 2020-05-18 17:59
 * @since 1.0.0
 */
public class OGGBaseBean implements Serializable  {
    protected String table;
    protected String op_type;
    protected String op_ts;
    protected String current_ts;
    protected String pos;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getOp_type() {
        return op_type;
    }

    public void setOp_type(String op_type) {
        this.op_type = op_type;
    }

    public String getOp_ts() {
        return op_ts;
    }

    public void setOp_ts(String op_ts) {
        this.op_ts = op_ts;
    }

    public String getCurrent_ts() {
        return current_ts;
    }

    public void setCurrent_ts(String current_ts) {
        this.current_ts = current_ts;
    }

    public String getPos() {
        return pos;
    }

    public void setPos(String pos) {
        this.pos = pos;
    }
}
