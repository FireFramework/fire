package com.zto.fire.demo.bean;

import com.zto.fire.demo.bean.jsonparse.BaseJson;

/**
 * 对应sjzn_spark_binlog_order_topic
 */
public class BinlogOrder implements BaseJson {
    private Long id;
    private String order_code;
    private String old_order_code;
    private String bill_code;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getOrder_code() {
        return order_code;
    }

    public void setOrder_code(String order_code) {
        this.order_code = order_code;
    }

    public String getOld_order_code() {
        return old_order_code;
    }

    public void setOld_order_code(String old_order_code) {
        this.old_order_code = old_order_code;
    }

    public String getBill_code() {
        return bill_code;
    }

    public void setBill_code(String bill_code) {
        this.bill_code = bill_code;
    }

    @Override
    public String toString() {
        return "BinlogOrder{" +
                "id=" + id +
                ", order_code='" + order_code + '\'' +
                ", old_order_code='" + old_order_code + '\'' +
                ", bill_code='" + bill_code + '\'' +
                '}';
    }
}
