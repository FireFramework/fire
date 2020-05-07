package com.zto.fire.demo.bean;

import java.io.Serializable;

/**
 * 用于封装OGG发送过来的json的基础类
 * Created by ChengLong on 2017-05-25.
 */
public class OGGBaseBean implements Serializable {
    public String table;
    public String op_type;
    public String op_ts;
    public String current_ts;
    public String pos;

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
