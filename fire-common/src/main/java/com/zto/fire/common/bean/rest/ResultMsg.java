package com.zto.fire.common.bean.rest;

import com.alibaba.fastjson.JSON;
import com.zto.fire.common.enu.ErrorCode;

/**
 * 返回消息
 *
 * @author ChengLong 2018年6月12日 13:42:23
 */
public class ResultMsg {
    // 消息体
    private Object content;
    // 系统错误码
    private ErrorCode code;
    // 错误描述
    private String msg;

    /**
     * 验证是否成功
     *
     * @param resultMsg
     * @return true: 成功 false 失败
     */
    public static boolean isSuccess(ResultMsg resultMsg) {
        return resultMsg != null && resultMsg.getCode() == ErrorCode.SUCCESS;
    }

    /**
     * 获取描述信息
     *
     * @param resultMsg
     * @return 描述信息
     */
    public static String getMsg(ResultMsg resultMsg) {
        if (resultMsg != null) {
            return resultMsg.getMsg();
        } else {
            return "";
        }
    }

    /**
     * 获取状态码
     *
     * @return 状态码
     */
    public static ErrorCode getCode(ResultMsg resultMsg) {
        if (resultMsg != null) {
            return resultMsg.getCode();
        }
        return ErrorCode.ERROR;
    }

    public ResultMsg() {
    }

    public ResultMsg(String content, ErrorCode code, String msg) {
        this.content = content;
        this.code = code;
        this.msg = msg;
    }

    public Object getContent() {
        return content;
    }

    public void setContent(Object content) {
        this.content = content;
    }

    public ErrorCode getCode() {
        return code;
    }

    public void setCode(ErrorCode code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    /**
     * 构建成功消息
     *
     * @param content
     * @param msg
     */
    public String buildSuccess(Object content, String msg) {
        this.content = content;
        this.code = ErrorCode.SUCCESS;
        this.msg = msg;
        return this.toString();
    }

    /**
     * 构建失败消息
     *
     * @param msg
     */
    public String buildError(String msg, ErrorCode errorCode) {
        this.content = "";
        this.code = errorCode;
        this.msg = msg;
        return this.toString();
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
