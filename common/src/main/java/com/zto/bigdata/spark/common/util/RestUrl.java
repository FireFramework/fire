package com.zto.bigdata.spark.common.util;

import com.zto.bigdata.spark.common.enu.YarnState;
import org.apache.commons.lang3.StringUtils;

import java.util.concurrent.atomic.AtomicReference;

/**
 * 用于封装restful接口地址
 *
 * @author ChengLong 2019-5-15 18:51:36
 */
public class RestUrl {
    public static final String yarnMasterIp = "192.168.25.180";
    public static final String yarnStandbyIp = "192.168.25.181";
    public static final String yarnWindowsIp = "10.9.46.107";

    // 用于存放当前的接口访问ip，如果发生master切换，则在此处变更
    private static AtomicReference<String> yarnRestUrl;

    static {
        if (SystemInfoUtils.isLinux()) {
            yarnRestUrl = new AtomicReference<>(StringsUtils.append("http://", yarnMasterIp, ":8088/ws/v1/cluster/apps"));
        } else {
            yarnRestUrl = new AtomicReference<>(StringsUtils.append("http://", yarnWindowsIp, ":8088/ws/v1/cluster/apps"));
        }
    }

    /**
     * 根据不同的状态获取接口地址
     *
     * @param state yarn程序运行的状态
     * @return URL地址
     */
    public static String yarnAppStateUrl(YarnState state) {
        if (StringUtils.isBlank(state.getState())) {
            return "";
        }
        return "?state=" + state.getState();
    }

    /**
     * 根据applicationId获取应用的信息
     *
     * @param applicationId yarn的applicationId
     * @return URL地址
     */
    public static String yarnAppUrl(String applicationId) {
        if (StringUtils.isBlank(applicationId)) {
            return "";
        }
        return "/" + applicationId;
    }

    /**
     * 当发生接口访问超时时，在catch到异常时切换currentIp地址
     */
    public static void changeYarnIp() {
        String url = yarnRestUrl.get();
        if (url.contains(yarnMasterIp)) {
            yarnRestUrl.set(url.replace(yarnMasterIp, yarnStandbyIp));
        } else {
            yarnRestUrl.set(url.replace(yarnStandbyIp, yarnMasterIp));
        }
    }

    /**
     * 获取yarn接口地址的前缀
     *
     * @return 返回接口url前缀
     */
    public static String getYarnRestPrefix() {
        return yarnRestUrl.get();
    }

}
