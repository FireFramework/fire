package com.zto.fire.demo.bean;

import com.alibaba.fastjson.JSON;
import com.zto.fire.common.bean.HBaseBaseBean;

public class OrderCommon extends HBaseBaseBean<OrderCommon> {
    private Long id;
    private String order_code;
    private String bill_code;
    private String use_site;
    private Long use_site_id;
    private String pro_date;
    private String pro_site;
    private String pro_site_id;
    private String emp_name;
    private String emp_code;
    private Long emp_id;
    private String cust_name;
    private String cust_code;
    private Long cust_id;
    private String record_date;
    private Long bl_online;
    private String pro_man;
    private String pro_man_id;
    private Long bl_use;
    private String use_date;
    private Long bl_lock;
    private Long platformid;
    private String des_site;
    private String spare_field1;
    private String des_site_id;
    private String remark;
    private String spare_field2;

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }

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

    public String getBill_code() {
        return bill_code;
    }

    public void setBill_code(String bill_code) {
        this.bill_code = bill_code;
    }

    public String getUse_site() {
        return use_site;
    }

    public void setUse_site(String use_site) {
        this.use_site = use_site;
    }

    public Long getUse_site_id() {
        return use_site_id;
    }

    public void setUse_site_id(Long use_site_id) {
        this.use_site_id = use_site_id;
    }

    public String getPro_date() {
        return pro_date;
    }

    public void setPro_date(String pro_date) {
        this.pro_date = pro_date;
    }

    public String getPro_site() {
        return pro_site;
    }

    public void setPro_site(String pro_site) {
        this.pro_site = pro_site;
    }

    public String getPro_site_id() {
        return pro_site_id;
    }

    public void setPro_site_id(String pro_site_id) {
        this.pro_site_id = pro_site_id;
    }

    public String getEmp_name() {
        return emp_name;
    }

    public void setEmp_name(String emp_name) {
        this.emp_name = emp_name;
    }

    public String getEmp_code() {
        return emp_code;
    }

    public void setEmp_code(String emp_code) {
        this.emp_code = emp_code;
    }

    public Long getEmp_id() {
        return emp_id;
    }

    public void setEmp_id(Long emp_id) {
        this.emp_id = emp_id;
    }

    public String getCust_name() {
        return cust_name;
    }

    public void setCust_name(String cust_name) {
        this.cust_name = cust_name;
    }

    public String getCust_code() {
        return cust_code;
    }

    public void setCust_code(String cust_code) {
        this.cust_code = cust_code;
    }

    public Long getCust_id() {
        return cust_id;
    }

    public void setCust_id(Long cust_id) {
        this.cust_id = cust_id;
    }

    public String getRecord_date() {
        return record_date;
    }

    public void setRecord_date(String record_date) {
        this.record_date = record_date;
    }

    public Long getBl_online() {
        return bl_online;
    }

    public void setBl_online(Long bl_online) {
        this.bl_online = bl_online;
    }

    public String getPro_man() {
        return pro_man;
    }

    public void setPro_man(String pro_man) {
        this.pro_man = pro_man;
    }

    public String getPro_man_id() {
        return pro_man_id;
    }

    public void setPro_man_id(String pro_man_id) {
        this.pro_man_id = pro_man_id;
    }

    public Long getBl_use() {
        return bl_use;
    }

    public void setBl_use(Long bl_use) {
        this.bl_use = bl_use;
    }

    public String getUse_date() {
        return use_date;
    }

    public void setUse_date(String use_date) {
        this.use_date = use_date;
    }

    public Long getBl_lock() {
        return bl_lock;
    }

    public void setBl_lock(Long bl_lock) {
        this.bl_lock = bl_lock;
    }

    public Long getPlatformid() {
        return platformid;
    }

    public void setPlatformid(Long platformid) {
        this.platformid = platformid;
    }

    public String getDes_site() {
        return des_site;
    }

    public void setDes_site(String des_site) {
        this.des_site = des_site;
    }

    public String getSpare_field1() {
        return spare_field1;
    }

    public void setSpare_field1(String spare_field1) {
        this.spare_field1 = spare_field1;
    }

    public String getDes_site_id() {
        return des_site_id;
    }

    public void setDes_site_id(String des_site_id) {
        this.des_site_id = des_site_id;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public String getSpare_field2() {
        return spare_field2;
    }

    public void setSpare_field2(String spare_field2) {
        this.spare_field2 = spare_field2;
    }

    @Override
    public OrderCommon buildRowKey() {
        this.rowKey = this.bill_code;
        return this;
    }
}
