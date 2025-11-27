/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zto.fire.common.util;

import com.zto.fire.common.conf.FireFrameworkConf;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 各种常用算法加密工具类
 *
 * @author ChengLong 2018年7月16日 09:53:59
 */
public class EncryptUtils {
    private static final String ERROR_MESSAGE = "参数不合法";
    private static final Logger logger = LoggerFactory.getLogger(EncryptUtils.class);

    private EncryptUtils() {}

    /**
     * BASE64解密
     */
    public static String base64Decrypt(String message) {
        Objects.requireNonNull(message, ERROR_MESSAGE);
        try {
            return new String(Base64.getDecoder().decode(message));
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
            return new String(Base64.getEncoder().encode(message.getBytes()));
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
            byte[] result = digest.digest(message.getBytes(StandardCharsets.UTF_8));
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
        String fireAuth = EncryptUtils.md5Encrypt(FireFrameworkConf.restServerSecret() + privateKey + DateFormatUtils.formatCurrentDate());
        return fireAuth.equals(auth);
    }

    /**
     * 生成RSA的公钥和私钥
     */
    public static Map<String, Object> genRSAKey() throws Exception {
        KeyPairGenerator keyPairGen = KeyPairGenerator.getInstance("RSA");
        keyPairGen.initialize(1024);
        KeyPair keyPair = keyPairGen.generateKeyPair();
        RSAPublicKey publicKey = (RSAPublicKey) keyPair.getPublic();
        RSAPrivateKey privateKey = (RSAPrivateKey) keyPair.getPrivate();
        Map<String, Object> keyMap = new HashMap<>(2);
        keyMap.put("RSAPublicKey", publicKey);
        keyMap.put("RSAPrivateKey", privateKey);
        return keyMap;
    }

    /**
     * 基于RSA加密算法对指定的文本进行加密
     * @param text
     * 待加密的问题吧
     * @param publicKeyStr
     * RSA公钥
     * @return
     * 加密后的字符串
     */
    public static String rsaEncrypt(String text, String publicKeyStr) {
        try {
            X509EncodedKeySpec x509EncodedKeySpec = new X509EncodedKeySpec(Base64.getDecoder().decode(publicKeyStr));
            KeyFactory factory = KeyFactory.getInstance("RSA");
            PublicKey key = factory.generatePublic(x509EncodedKeySpec);
            Cipher cipher = Cipher.getInstance("RSA");
            cipher.init(Cipher.ENCRYPT_MODE, key);
            return Base64.getEncoder().encodeToString(cipher.doFinal(text.getBytes("UTF-8")));
        } catch (Exception e) {
            logger.error("基于RSA算法进行加密报错", e);
            return null;
        }
    }

    /**
     * 基于RSA加密算法对指定的文本进行解密
     * @param text
     * 加密后的文本
     * @param privateKeyStr
     * RSA私钥
     * @return
     * 解密后的文本
     */
    public static String rsaDecrypt(String text, String privateKeyStr) {
        try {
            PKCS8EncodedKeySpec pkcs8EncodedKeySpec = new PKCS8EncodedKeySpec(Base64.getDecoder().decode(privateKeyStr));
            KeyFactory factory = KeyFactory.getInstance("RSA");
            PrivateKey key = factory.generatePrivate(pkcs8EncodedKeySpec);
            Cipher cipher = Cipher.getInstance("RSA");
            cipher.init(Cipher.DECRYPT_MODE, key);
            return new String(cipher.doFinal(Base64.getDecoder().decode(text)));
        } catch (Exception e) {
            logger.warn("基于RSA算法进行解密报错，密文：{} {}", text, e.getMessage());
            return null;
        }
    }

}
