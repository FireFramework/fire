package com.zto.fire.demo.bean;

/**
 * zto_site_senda_bills表for OGG
 * Created by ChengLong on 2017-05-25.
 */
public class ZtoSiteSendaBillsOGG extends OGGBaseBean {
    public Senda after;

    public Senda getAfter() {
        return after;
    }

    public void setAfter(Senda after) {
        this.after = after;
    }

    @Override
    public String toString() {
        return "ZtoSiteSendaBillsOGG{" +
                "after=" + after +
                '}';
    }
}
