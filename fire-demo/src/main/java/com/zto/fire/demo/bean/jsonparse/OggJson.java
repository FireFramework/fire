package com.zto.fire.demo.bean.jsonparse;

import java.io.Serializable;

public class OggJson<T> implements BaseJson {
    private String table;
    private String op_type;
    private String op_ts;
    private String current_ts;
    private String gtid;
    private String logFile;
    private String offset;
    private String schema;
    private String when;
    private String pos;
    private T after;
    private T before;

    public OggJson() {
    }

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

    public String getGtid() {
        return gtid;
    }

    public void setGtid(String gtid) {
        this.gtid = gtid;
    }

    public String getLogFile() {
        return logFile;
    }

    public void setLogFile(String logFile) {
        this.logFile = logFile;
    }

    public String getOffset() {
        return offset;
    }

    public void setOffset(String offset) {
        this.offset = offset;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getWhen() {
        return when;
    }

    public void setWhen(String when) {
        this.when = when;
    }

    public String getPos() {
        return pos;
    }

    public void setPos(String pos) {
        this.pos = pos;
    }

    public T getAfter() {
        return after;
    }

    public OggJson setAfter(T after) {
        this.after = after;
        return this;
    }

    public T getBefore() {
        return before;
    }

    public OggJson setBefore(T before) {
        this.before = before;
        return this;
    }
}
