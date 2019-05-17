package com.zto.bigdata.spark.common.rest;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zto.bigdata.spark.common.bean.rest.yarn.App;
import com.zto.bigdata.spark.common.enu.YarnState;
import com.zto.bigdata.spark.common.util.HttpClientUtils;
import com.zto.bigdata.spark.common.util.RestUrl;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * yarn相关接口
 *
 * @author ChengLong 2019-5-15 19:21:56
 */
public class YarnRest {

    /**
     * 指定url地址调用yarn接口
     *
     * @param urlSuffix 接口的url地址的后缀
     * @return 接口调用返回的json数据
     */
    private static String invokeYarnRest(String urlSuffix, String json, String method) {
        String response = "";
        if (StringUtils.isBlank(method) || "get".equalsIgnoreCase(method)) {
            try {
                response = HttpClientUtils.doGet(RestUrl.getYarnRestPrefix() + urlSuffix);
            } catch (Exception e) {
                RestUrl.changeYarnIp();
                response = HttpClientUtils.doGetIgnore(RestUrl.getYarnRestPrefix() + urlSuffix);
            }
        } else if ("put".equalsIgnoreCase(method)) {
            try {
                response = HttpClientUtils.doPut(RestUrl.getYarnRestPrefix() + urlSuffix, json);
            } catch (Exception e) {
                RestUrl.changeYarnIp();
                response = HttpClientUtils.doPutIgnore(RestUrl.getYarnRestPrefix() + urlSuffix, json);
            }
        } else if ("post".equalsIgnoreCase(method)) {
            try {
                response = HttpClientUtils.doPost(RestUrl.getYarnRestPrefix() + urlSuffix, json);
            } catch (Exception e) {
                RestUrl.changeYarnIp();
                response = HttpClientUtils.doPostIgnore(RestUrl.getYarnRestPrefix() + urlSuffix, json);
            }
        }
        return response;
    }

    /**
     * 根据状态获取yarn中的job
     *
     * @param state 任务的状态，为空则返回所有
     * @return 将yarn接口返回的json解析为App对象
     */
    public static List<App> getAppInfoByState(YarnState state) {
        // 调用yarn的state接口
        String json = invokeYarnRest(RestUrl.yarnAppStateUrl(state), "", RequestMethod.GET().toString());

        if (StringUtils.isNotBlank(json)) {
            JSONObject obj = JSON.parseObject(json).getJSONObject("apps");
            if (obj != null) {
                JSONArray appArray = obj.getJSONArray("app");
                if (appArray != null && appArray.size() > 0) {
                    List<App> appList = new ArrayList<>(appArray.size());
                    for (int i = 0; i < appArray.size(); i++) {
                        appList.add(appArray.getObject(i, App.class));
                    }
                    return appList;
                }
            }
        }

        return Collections.emptyList();
    }

    /**
     * 根据applicationId获取app信息
     *
     * @param applicationId yarn的applicationId
     * @return job的json数据
     */
    public static App getAppInfoById(String applicationId) {
        if (StringUtils.isBlank(applicationId)) {
            return null;
        }
        String json = invokeYarnRest(RestUrl.yarnAppUrl(applicationId), "", RequestMethod.GET().toString());
        if (StringUtils.isBlank(json)) {
            return null;
        }
        JSONObject jsonObject = JSON.parseObject(json);
        if (jsonObject == null) {
            return null;
        }
        return jsonObject.getObject("app", App.class);
    }

    /**
     * 根据applicationId获取job在yarn上的job信息
     *
     * @param applicationId job的applicationId
     * @return app信息
     */
    public static App getAppStateById(String applicationId) {
        if (StringUtils.isBlank(applicationId)) {
            return null;
        }
        return getAppInfoById(applicationId);
    }

    /**
     * 根据appName与任务的状态，获取最新一个job的app信息
     *
     * @param appName job的名称
     * @param state job的状态
     * @return app信息
     */
    public static App getLastAppStateByName(String appName, YarnState state) {
        if (StringUtils.isBlank(appName)) {
            return null;
        }
        List<App> appList = getAppInfoByState(state);
        if (appList == null || appList.size() == 0) {
            return null;
        }
        // 按照startTime排序，取最新一条
        Collections.sort(appList);
        for (App app : appList) {
            if (app == null || StringUtils.isBlank(app.getName())) {
                return null;
            }
            if (app.getName().trim().equalsIgnoreCase(appName.trim())) {
                return app;
            }
        }

        return null;
    }

    /**
     * 根据applicationId kill任务
     *
     * @param applicationId applicationId job的applicationId
     */
    public static String killAppById(String applicationId) {
        if (StringUtils.isBlank(applicationId)) {
            return "";
        }
        String json = "{\"state\":\"KILLED\"}";

        return invokeYarnRest(RestUrl.yarnAppUrl(applicationId) + "/state", json, RequestMethod.PUT().toString());
    }


    public static void main(String[] args) throws Exception {
        /*long start = System.currentTimeMillis();
        List<App> list = getAppInfoByState(YarnState.NULL);
        System.out.println(list.size() + " 耗时：" + (System.currentTimeMillis() - start));
        start = System.currentTimeMillis();
        list = getAppInfoByState(YarnState.RUNNING);
        System.out.println(list.size() + " 耗时：" + (System.currentTimeMillis() - start));
        System.out.println(getAppStateById("application_1557472996243_3449"));
        System.out.println(killAppById("application_1557472996243_3486"));*/
        System.out.println(getLastAppStateByName("test", YarnState.RUNNING).getId());
    }
}
