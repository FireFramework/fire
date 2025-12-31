package com.zto.fire.examples.flink.serialize;

import com.zto.fire.mq.deserializer.BaseDeserializeEntity;

import java.io.Serializable;
import java.util.Date;

public class OrderInfo extends BaseDeserializeEntity {
    private Integer id;
    private String name;
    private Date createTime;
    private Double money;

    public OrderInfo() {
    }

    public OrderInfo(Integer id, String name, Date createTime, Double money) {
        this.id = id;
        this.name = name;
        this.createTime = createTime;
        this.money = money;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Double getMoney() {
        return money;
    }

    public void setMoney(Double money) {
        this.money = money;
    }

    @Override
    public String toString() {
        return "OrderInfo{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", createTime=" + createTime +
                ", money=" + money +
                '}';
    }
}
