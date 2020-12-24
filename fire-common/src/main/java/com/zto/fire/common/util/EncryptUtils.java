package com.zto.fire.common.util;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/**
 * 各种常用算法加密工具类
 *
 * @author ChengLong 2018年7月16日 09:53:59
 */
public class EncryptUtils {
    private static final String SECRET = "($zto%-%fire$)";
    private static final String ERROR_MESSAGE = "参数不合法";
    private static final Logger logger = LoggerFactory.getLogger(EncryptUtils.class);

    private EncryptUtils() {}

    /**
     * BASE64解密
     */
    public static String base64Decrypt(String message) {
        Objects.requireNonNull(message, ERROR_MESSAGE);
        try {
            return new String((new BASE64Decoder()).decodeBuffer(message), StandardCharsets.UTF_8);
        } catch (Exception e) {
            logger.error("BASE64解密出错", e);
        }
        return "";
    }

    /**
     * BASE64加密
     */
    public static String base64Encrypt(String message) {
        Objects.requireNonNull(message, ERROR_MESSAGE);
        try {
            return new BASE64Encoder().encodeBuffer(message.getBytes());
        } catch (Exception e) {
            logger.error("BASE64加密出错", e);
        }
        return "";
    }

    /**
     * 生成32位md5码
     */
    public static String md5Encrypt(String message) {
        Objects.requireNonNull(message, ERROR_MESSAGE);
        try {
            // 得到一个信息摘要器
            MessageDigest digest = MessageDigest.getInstance("md5");
            byte[] result = digest.digest(message.getBytes());
            StringBuilder buffer = new StringBuilder();
            for (byte b : result) {
                int number = b & 0xff;// 加盐
                String str = Integer.toHexString(number);
                if (str.length() == 1) {
                    buffer.append('0');
                }
                buffer.append(str);
            }
            // 标准的md5加密后的结果
            return buffer.toString();
        } catch (NoSuchAlgorithmException e) {
            logger.error("生成32位md5码出错", e);
        }
        return "";
    }

    /**
     * SHA加密
     */
    public static String shaEncrypt(String message, String key) {
        Objects.requireNonNull(message, ERROR_MESSAGE);
        if(StringUtils.isBlank(key)) {
            key = "SHA";
        }
        try {
            MessageDigest sha = MessageDigest.getInstance(key);
            sha.update(message.getBytes(StandardCharsets.UTF_8));
            return new BigInteger(sha.digest()).toString(32);
        } catch (Exception e) {
            logger.error("生成SHA加密出错", e);
        }
        return "";
    }

    /**
     * header权限校验
     * @param auth
     * 请求json
     * @return
     * true：身份合法  false：身份非法
     */
    public static boolean checkAuth(String auth, String privateKey) {
        if (StringUtils.isBlank(auth)) {
            return false;
        }
        String fireAuth = EncryptUtils.md5Encrypt(SECRET + privateKey + DateFormatUtils.formatCurrentDate());
        return fireAuth.equals(auth);
    }
}
