package com.zto.bigdata.spark.common.util;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.URI;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.lang3.StringUtils;

/**
 * HTTP接口调用，各模块继承自该类
 * Created by ChengLong on 2017-12-12.
 */
public class HttpClientUtils {
    private HttpClient httpClient = new HttpClient();
    private GetMethod getMethod = new GetMethod();
    private PostMethod postMethod = new PostMethod();

    public HttpClientUtils() {
        // 设置 Http 连接超时为5秒
        httpClient.getHttpConnectionManager().getParams().setConnectionTimeout(3000);
        // 设置 get 请求超时为 5 秒
        getMethod.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, 3000);
        // 设置请求重试处理，用的是默认的重试处理：请求三次
        getMethod.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler());
        postMethod.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, 3000);
        postMethod.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler());
    }

    /**
     * HTTP通用接口调用（Get请求）
     *
     * @param url 地址
     * @return 调用结果
     * @throws Exception
     */
    public String httpGetInvoke(String url) throws Exception {
        getMethod.setURI(new URI(url, true, "utf-8"));
        int statusCode = httpClient.executeMethod(getMethod);
        // 判断访问的状态码
        if (statusCode != HttpStatus.SC_OK) {
            System.err.println("请求出错: " + getMethod.getStatusLine());
        }
        // 读取 HTTP 响应内容，这里简单打印网页内容
        byte[] responseBody = getMethod.getResponseBody();
        return new String(responseBody, "utf-8");
    }

    /**
     * HTTP通用接口调用（Post请求）
     *
     * @param url 地址
     * @return 调用结果
     * @throws Exception
     */
    public String httpPostInvoke(String url, String json) throws Exception {
        postMethod.setURI(new URI(url, true, "utf-8"));
        postMethod.addRequestHeader("Content-Type", "application/json");
        if(json != null && StringUtils.isNotBlank(json.trim())) {
            RequestEntity requestEntity = new StringRequestEntity(json, "application/json", "UTF-8");
            postMethod.setRequestHeader("Content-Length", String.valueOf(requestEntity.getContentLength()));
            postMethod.setRequestEntity(requestEntity);
        }
        httpClient.executeMethod(postMethod);
        String responses= postMethod.getResponseBodyAsString();
        return responses;
    }

    /**
     * 释放连接
     */
    public void releaseGetConnection() {
        this.getMethod.releaseConnection();
    }

    public void releasePostConnection() {
        this.postMethod.releaseConnection();
    }

    public void releaseConnection() {
        this.getMethod.releaseConnection();
        this.postMethod.releaseConnection();
    }
}
