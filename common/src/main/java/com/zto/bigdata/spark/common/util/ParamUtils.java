package com.zto.bigdata.spark.common.util;

import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 参数合法性检查
 * Created by ChengLong on 2017-03-30.
 */
public class ParamUtils {

    /**
     * 集合为空检查
     *
     * @param collection
     * @return
     */
    public static boolean isNotBlank(Collection collection) {
        if (collection == null || collection.size() == 0) {
            return false;
        }
        return true;
    }

    /**
     * 集合为空检查
     *
     * @param collection
     * @return
     */
    public static boolean isBlank(Collection collection) {
        return !isNotBlank(collection);
    }

    /**
     * map合法性检查
     *
     * @param map
     * @return
     */
    public static boolean isNotBlank(Map map) {
        if (map == null || map.size() == 0) {
            return false;
        }
        return true;
    }

    /**
     * map合法性检查
     *
     * @param map
     * @return
     */
    public static boolean isBlank(Map map) {
        return !isNotBlank(map);
    }


    /**
     * 方法参数或数组合法性检查
     *
     * @param params
     * @return
     */
    public static boolean isNotBlank(Object... params) {
        if (params == null || params.length == 0) {
            return false;
        }
        for (Object param : params) {
            if (param == null) {
                return false;
            }
        }
        return true;
    }

    /**
     * 方法参数或数组合法性检查
     *
     * @param params
     * @return
     */
    public static boolean isBlank(Object... params) {
        return !isNotBlank(params);
    }

    /**
     * 提前字符串中${}中的内容
     *
     * @param str 含有${}的字符串
     * @return ${}中的内容列表
     */
    public static Set<String> extractParams(String str) {
        if (StringUtils.isBlank(str)) {
            return Collections.EMPTY_SET;
        }
        Matcher m = Pattern.compile("\\$\\{\\s*\\w+\\s*\\}").matcher(str);

        Set<String> paramSet = new LinkedHashSet<String>();
        while (m.find()) {
            String param = m.group();
            if (StringUtils.isNotBlank(param)) {
                String paramName = param.replace("${", "").replace("}", "").trim();
                if (StringUtils.isNotBlank(paramName)) {
                    paramSet.add(paramName);
                }
            }
        }
        return paramSet;
    }

    /**
     * 参数非空约束
     *
     * @param params
     */
    public void requireNonNull(Object... params) {
        if (params != null && params.length > 0) {
            for (Object param : params) {
                Objects.requireNonNull(param, "参数不能为空");
            }
        }
    }
}
