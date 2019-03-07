package com.zto.bigdata.spark.bean;

/**
 * zto_site_senda_bills表for OGG
 * Created by ChengLong on 2017-05-25.
 */
public class ZtoSiteSendaBillsOGG extends OGGBaseBean {
    public SiteSendMqDTO after;

    public SiteSendMqDTO getAfter() {
        return after;
    }

    public void setAfter(SiteSendMqDTO after) {
        this.after = after;
    }

    @Override
    public String toString() {
        return "ZtoSiteSendaBillsOGG{" +
                "after=" + after +
                '}';
    }
}
