package com.zto.bigdata.spark.common.util;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * 字符串工具类
 *
 * @author ChengLong 2019-4-11 09:06:26
 */
public class StringsUtils {

    /**
     * 处理成超链接
     *
     * @param str
     * @return
     */
    public static String hrefTag(String str) {
        return append("<a href='", str, "'>", str, "</a>");
    }

    /**
     * 追加换行
     *
     * @param str
     * @return
     */
    public static String brTag(String str) {
        return append(str, "<br/>");
    }

    /**
     * 字符串拼接
     *
     * @param strs 多个字符串
     * @return 拼接结果
     */
    public static String append(String... strs) {
        StringBuilder sb = new StringBuilder("");
        if (null != strs && strs.length > 0) {
            for (String str : strs) {
                sb.append(str);
            }
        }

        return sb.toString();
    }

    /**
     * replace多组字符串中的数据
     *
     * @param map
     * @return
     * @apiNote replace(str, ImmutableMap.of ( " # ", " ", ", ", " "))
     */
    public static String replace(String str, Map<String, String> map) {
        if (StringUtils.isNotBlank(str) && null != map && map.size() > 0) {
            for (Map.Entry<String, String> entry : map.entrySet()) {
                str = str.replace(entry.getKey(), entry.getValue());
            }
        }
        return str;
    }

    public static void main(String[] args) {
        String str = "#,$@,#";
        System.out.println(replace(str, ImmutableMap.of("#", "", ",", "")));
    }
}
