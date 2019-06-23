package com.zto.fire.common.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Yarn相关工具类
 * @author ChengLong 2018年8月10日 16:03:29
 */
public class YarnUtils {
    /**
     * 使用正则提取日志中的applicationId
     * @param log
     * @return
     */
    public static String getAppId(String log) {
        // 正则表达式规则
        String regEx = "application_[0-9]+_[0-9]+";
        // 编译正则表达式
        Pattern pattern = Pattern.compile(regEx);
        // 忽略大小写的写法
        // Pattern pat = Pattern.compile(regEx, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(log);
        // 查找字符串中是否有匹配正则表达式的字符/字符串
        if(matcher.find()) {
            return matcher.group();
        } else {
            return "";
        }
    }
}
