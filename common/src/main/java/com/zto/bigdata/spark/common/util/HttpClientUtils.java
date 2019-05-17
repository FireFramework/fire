package com.zto.bigdata.spark.common.util;

import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.methods.*;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.lang3.StringUtils;

/**
 * HTTP接口调用，各模块继承自该类
 * Created by ChengLong on 2017-12-12.
 */
public class HttpClientUtils {

    /**
     * HTTP通用接口调用（Get请求）
     *
     * @param url 地址
     * @return 调用结果
     */
    public static String doGet(String url) throws Exception {
        byte[] responseBody = null;
        GetMethod getMethod = null;
        try {
            getMethod = new GetMethod();
            // 设置 get 请求超时为 5 秒
            getMethod.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, 3000);
            // 设置请求重试处理，用的是默认的重试处理：请求三次
            getMethod.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler());

            getMethod.setURI(new URI(url, true, "utf-8"));
            HttpClient httpClient = new HttpClient();
            int statusCode = httpClient.executeMethod(getMethod);
            // 判断访问的状态码
            if (statusCode != HttpStatus.SC_OK) {
                System.err.println("请求出错: " + getMethod.getStatusLine());
            }
            // 读取 HTTP 响应内容，这里简单打印网页内容
            responseBody = getMethod.getResponseBody();
        } catch (Exception e) {
            throw e;
        } finally {
            if (getMethod != null) {
                getMethod.releaseConnection();
            }
        }
        return new String(responseBody, "utf-8");
    }

    /**
     * HTTP通用接口调用（Post请求）
     *
     * @param url 地址
     * @return 调用结果
     */
    public static String doPost(String url, String json) throws Exception {
        String responses = "";
        PostMethod postMethod = null;
        try {
            postMethod = new PostMethod();
            postMethod.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, 3000);
            postMethod.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler());
            postMethod.setURI(new URI(url, true, "utf-8"));
            postMethod.addRequestHeader("Content-Type", "application/json");
            if (json != null && StringUtils.isNotBlank(json.trim())) {
                RequestEntity requestEntity = new StringRequestEntity(json, "application/json", "UTF-8");
                postMethod.setRequestHeader("Content-Length", String.valueOf(requestEntity.getContentLength()));
                postMethod.setRequestEntity(requestEntity);
            }
            HttpClient httpClient = new HttpClient();
            httpClient.executeMethod(postMethod);
            responses = postMethod.getResponseBodyAsString();
        } catch (Exception e) {
            throw e;
        } finally {
            if (postMethod != null) {
                postMethod.releaseConnection();
            }
        }
        return responses;
    }

    /**
     * 发送一次post请求到指定的地址，不向上抛出异常
     *
     * @param url 接口地址
     * @return 调用结果
     */
    public static String doPut(String url, String json) throws Exception {
        String resStr = null;
        PutMethod putMethod = null;
        try {
            HttpClient htpClient = new HttpClient();
            putMethod = new PutMethod();
            putMethod.setURI(new URI(url, true, "utf-8"));
            putMethod.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, 3000);
            putMethod.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler());
            if (json != null && StringUtils.isNotBlank(json.trim())) {
                RequestEntity requestEntity = new StringRequestEntity(json, "application/json", "UTF-8");
                putMethod.setRequestHeader("Content-Length", String.valueOf(requestEntity.getContentLength()));
                putMethod.setRequestEntity(requestEntity);
            }
            int statusCode = htpClient.executeMethod(putMethod);
            if (statusCode != HttpStatus.SC_OK) {
                return "";
            }
            byte[] responseBody = putMethod.getResponseBody();
            resStr = new String(responseBody, "utf-8");
        } catch (Exception e) {
            throw e;
        } finally {
            if (putMethod != null) {
                putMethod.releaseConnection();
            }
        }
        return resStr;
    }

    /**
     * 发送一次get请求到指定的地址，不向上抛出异常
     *
     * @param url 接口地址
     * @return 调用结果
     */
    public static String doGetIgnore(String url) {
        String response = "";
        try {
            response = doGet(url);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            return response;
        }
    }

    /**
     * 发送一次post请求到指定的地址，不向上抛出异常
     *
     * @param url 接口地址
     * @return 调用结果
     */
    public static String doPostIgnore(String url, String json) {
        String response = "";
        try {
            response = doPost(url, json);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            return response;
        }
    }

    /**
     * 发送一次put请求到指定的地址，不向上抛出异常
     *
     * @param url 接口地址
     * @return 调用结果
     */
    public static String doPutIgnore(String url, String json) {
        String response = "";
        try {
            response = doPut(url, json);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            return response;
        }
    }
}
