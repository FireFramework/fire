package com.zto.fire.common.util;

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

    /**
     * 16进制的字符串表示转成字节数组
     *
     * @param hexString
     *            16进制格式的字符串
     * @return 转换后的字节数组
     **/
    public static byte[] toByteArray(String hexString) {
        if (StringUtils.isEmpty(hexString))
            throw new IllegalArgumentException("this hexString must not be empty");

        hexString = hexString.toLowerCase();
        final byte[] byteArray = new byte[hexString.length() / 2];
        int k = 0;
        for (int i = 0; i < byteArray.length; i++) {//因为是16进制，最多只会占用4位，转换成字节需要两个16进制的字符，高位在先
            byte high = (byte) (Character.digit(hexString.charAt(k), 16) & 0xff);
            byte low = (byte) (Character.digit(hexString.charAt(k + 1), 16) & 0xff);
            byteArray[i] = (byte) (high << 4 | low);
            k += 2;
        }
        return byteArray;
    }

    /**
     * 字节数组转成16进制表示格式的字符串
     *
     * @param byteArray
     *            需要转换的字节数组
     * @return 16进制表示格式的字符串
     **/
    public static String toHexString(byte[] byteArray) {
        if (byteArray == null || byteArray.length < 1)
            throw new IllegalArgumentException("this byteArray must not be null or empty");

        final StringBuilder hexString = new StringBuilder();
        for (int i = 0; i < byteArray.length; i++) {
            if ((byteArray[i] & 0xff) < 0x10)//0~F前面不零
                hexString.append("0");
            hexString.append(Integer.toHexString(0xFF & byteArray[i]));
        }
        return hexString.toString().toLowerCase();
    }

    public static void main(String[] args) {
        String str = "#,$@,#";
        System.out.println(replace(str, ImmutableMap.of("#", "", ",", "")));
    }
}
