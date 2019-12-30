package com.zto.fire.common.util;

import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.methods.*;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * HTTP接口调用，各模块继承自该类
 * Created by ChengLong on 2017-12-12.
 */
public class HttpClientUtils {

    /**
     * 添加header请求信息
     *
     * @param method  请求的方式
     * @param headers 请求头信息
     */
    private static void setHeaders(HttpMethodBase method, Header... headers) {
        if (method != null && headers != null && headers.length > 0) {
            for (Header header : headers) {
                if (header != null) method.setRequestHeader(header);
            }
        }
    }

    /**
     * 以流的方式获取返回的消息体
     */
    private static String responseBody(HttpMethodBase method) throws Exception {
        if (method == null) return "";

        BufferedReader reader = new BufferedReader(new InputStreamReader(method.getResponseBodyAsStream()));
        StringBuffer stringBuffer = new StringBuffer();
        String str = "";
        while ((str = reader.readLine()) != null) {
            stringBuffer.append(str);
        }
        return stringBuffer.toString();
    }

    /**
     * HTTP通用接口调用（Get请求）
     *
     * @param url 地址
     * @return 调用结果
     */
    public static String doGet(String url, Header... headers) throws Exception {
        String responseBody = "";
        GetMethod getMethod = null;
        HttpClient httpClient = new HttpClient();
        try {
            getMethod = new GetMethod();
            // 设置 get 请求超时为 5 秒
            getMethod.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, 3000);
            // 设置请求重试处理，用的是默认的重试处理：请求三次
            getMethod.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler());
            // 设置请求头
            setHeaders(getMethod, headers);

            getMethod.setURI(new URI(url, true, "utf-8"));
            int statusCode = httpClient.executeMethod(getMethod);
            // 判断访问的状态码
            if (statusCode != HttpStatus.SC_OK) {
                System.err.println("请求出错: " + getMethod.getStatusLine());
            }
            // 读取 HTTP 响应内容，这里简单打印网页内容
            responseBody = responseBody(getMethod);
        } catch (Exception e) {
            throw e;
        } finally {
            if (getMethod != null) {
                getMethod.releaseConnection();
            }
            httpClient.getHttpConnectionManager().closeIdleConnections(0);
        }
        return responseBody;
    }

    /**
     * HTTP通用接口调用（Post请求）
     *
     * @param url 地址
     * @return 调用结果
     */
    public static String doPost(String url, String json, Header... headers) throws Exception {
        String responses = "";
        PostMethod postMethod = null;
        HttpClient httpClient = new HttpClient();
        try {
            postMethod = new PostMethod();
            httpClient.setConnectionTimeout(10000);
            postMethod.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, 10000);
            postMethod.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler());
            // 设置请求头
            setHeaders(postMethod, headers);
            postMethod.setURI(new URI(url, true, "utf-8"));
            postMethod.addRequestHeader("Content-Type", "application/json");
            if (json != null && StringUtils.isNotBlank(json.trim())) {
                RequestEntity requestEntity = new StringRequestEntity(json, "application/json", "UTF-8");
                postMethod.setRequestHeader("Content-Length", String.valueOf(requestEntity.getContentLength()));
                postMethod.setRequestEntity(requestEntity);
            }
            httpClient.executeMethod(postMethod);
            responses = responseBody(postMethod);
        } catch (Exception e) {
            throw e;
        } finally {
            if (postMethod != null) {
                postMethod.releaseConnection();
            }
            httpClient.getHttpConnectionManager().closeIdleConnections(0);
        }
        return responses;
    }

    /**
     * 发送一次post请求到指定的地址，不向上抛出异常
     *
     * @param url 接口地址
     * @return 调用结果
     */
    public static String doPut(String url, String json, Header... headers) throws Exception {
        String responseBody = "";
        PutMethod putMethod = null;
        HttpClient htpClient = new HttpClient();
        try {
            putMethod = new PutMethod();
            putMethod.setURI(new URI(url, true, "utf-8"));
            putMethod.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, 3000);
            putMethod.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler());
            // 设置请求头
            setHeaders(putMethod, headers);
            if (json != null && StringUtils.isNotBlank(json.trim())) {
                RequestEntity requestEntity = new StringRequestEntity(json, "application/json", "UTF-8");
                putMethod.setRequestHeader("Content-Length", String.valueOf(requestEntity.getContentLength()));
                putMethod.setRequestEntity(requestEntity);
            }
            int statusCode = htpClient.executeMethod(putMethod);
            if (statusCode != HttpStatus.SC_OK) {
                return "";
            }
            responseBody = responseBody(putMethod);
        } catch (Exception e) {
            throw e;
        } finally {
            if (putMethod != null) {
                putMethod.releaseConnection();
            }
            htpClient.getHttpConnectionManager().closeIdleConnections(0);
        }
        return responseBody;
    }

    /**
     * 发送一次get请求到指定的地址，不向上抛出异常
     *
     * @param url 接口地址
     * @return 调用结果
     */
    public static String doGetIgnore(String url, Header... headers) {
        String response = "";
        try {
            response = doGet(url, headers);
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
    public static String doPostIgnore(String url, String json, Header... headers) {
        String response = "";
        try {
            response = doPost(url, json, headers);
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
    public static String doPutIgnore(String url, String json, Header... headers) {
        String response = "";
        try {
            response = doPut(url, json, headers);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            return response;
        }
    }

}
