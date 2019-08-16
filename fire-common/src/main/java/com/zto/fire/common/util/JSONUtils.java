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

}
