package com.zto.fire.examples.bean;

import com.zto.fire.hbase.bean.HBaseBaseBean;
import org.apache.commons.lang3.StringUtils;

public class ScanCome extends HBaseBaseBean<ScanCome> {
    private String bill_code;

    private String json;


    public String getJson() {
        return json;
    }

    public void setJson(String json) {
        this.json = json;
    }

    public ScanCome() {
    }

    public ScanCome(String bill_code, String json) {
        this.bill_code = bill_code;
        this.json = json;
    }

    @Override
    public ScanCome buildRowKey() {
        this.rowKey = StringUtils.reverse(this.bill_code);
        return this;
    }
}
