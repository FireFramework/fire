package com.zto.fire.common.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;

/**
 * JSON工具类
 *
 * @author ChengLong 2019-8-16 09:39:56
 */
public class JSONUtils {

    /**
     * 解析JSON，并获取指定key对应的值
     *
     * @param json         json字符串
     * @param key          json的key
     * @param defaultValue 获取的key对应的值不存在或者为空，则返回默认值
     * @return value
     */
    public static <T> T getValue(String json, String key, T defaultValue) {
        if (StringUtils.isBlank(json) || StringUtils.isBlank(key)) {
            return null;
        }
        JSONObject paramObject = JSON.parseObject(json);
        if (paramObject == null) {
            return null;
        }
        return (T) paramObject.getOrDefault(key, defaultValue);
    }

    /**
     * 用于快速判断给定的字符串是否为合法的json
     * 注：不会验证每个field的合法性，仅做简单校验
     * @param json
     * 待校验的字符串
     * @return
     * true: 合法的字符串 false：非法的json字符串
     */
    public static boolean isJson(String json) {
        String jsonStr = StringUtils.trim(json);
        if (StringUtils.isBlank(jsonStr)) return false;
        if (jsonStr.startsWith("{") && jsonStr.endsWith("}")) return true;
        return false;
    }

    /**
     * 用于快速判断给定的字符串是否为合法的JsonArray
     * 注：不会验证每个field的合法性，仅做简单校验
     * @param jsonArray
     * 待校验的字符串
     * @return
     * true: 合法的字符串 false：非法的json字符串
     */
    public static boolean isJsonArray(String jsonArray) {
        String jsonArrayStr = StringUtils.trim(jsonArray);
        if (StringUtils.isBlank(jsonArrayStr)) return false;
        if (jsonArrayStr.startsWith("[") && jsonArrayStr.endsWith("]")) return true;
        return false;
    }

    /**
     * 用于快速判断给定的字符串是否为合法的JsonArray或json
     * 注：不会验证每个field的合法性，仅做简单校验
     * @param json
     * 待校验的字符串
     * @return
     * true: 合法的字符串 false：非法的json字符串
     */
    public static boolean checkJson(String json) {
        return isJson(json) || isJsonArray(json);
    }
}
