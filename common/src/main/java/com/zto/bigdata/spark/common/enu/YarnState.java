package com.zto.bigdata.spark.common.enu;

import org.apache.commons.lang3.StringUtils;

/**
 * yarn的job状态
 *
 * @author ChengLong 2019-5-16 09:19:56
 */
public enum YarnState {
    RUNNING("running"),
    ACCEPTED("accepted"),
    SUBMITTED("submitted"),
    FINISHED("finished"),
    FAILED("failed"),
    KILLED("killed"),
    UNDEFINED("undefined"),
    NULL(""),
    UNKONOW("unknow");

    // 状态信息
    private final String state;

    YarnState(String state) {
        this.state = state;
    }

    public String getState() {
        return state;
    }

    /**
     * 根据状态字符串返回状态枚举
     *
     * @param state 状态
     * @return
     */
    public static YarnState getState(String state) {
        if (StringUtils.isBlank(state)) {
            return NULL;
        }

        switch (state.toLowerCase()) {
            case "running":
                return RUNNING;
            case "accepted":
                return ACCEPTED;
            case "submitted":
                return SUBMITTED;
            case "finished":
                return FINISHED;
            case "failed":
                return FAILED;
            case "killed":
                return KILLED;
            default:
                return NULL;
        }
    }
}
