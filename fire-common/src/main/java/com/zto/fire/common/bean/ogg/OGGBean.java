package com.zto.fire.common.bean.ogg;

/**
 * 用于封装OGG发送过来的json的基础类
 * Created by ChengLong on 2017-05-25.
 */
public class OGGBean<T> extends OGGBaseBean {
    private T before;
    private T after;

    public T getBefore() {
        return before;
    }

    public void setBefore(T before) {
        this.before = before;
    }

    public T getAfter() {
        return after;
    }

    public void setAfter(T after) {
        this.after = after;
    }
}
