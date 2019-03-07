package com.zto.bigdata.spark.common.util;


import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.net.URI;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * HTTPS接口调用，各模块继承自该类
 * @author ChengLong 2018年7月16日 09:39:56
 */
public class HttpsClientUtils {
	private RequestConfig requestConfig = null;
	private CloseableHttpClient httpClient = null;
	private HttpPost httpPost = new HttpPost();
	private HttpGet httpGet = new HttpGet();
	private static final String CHARSET = "UTF-8";

	public HttpsClientUtils() {
		// 设置 Http 连接超时为5秒
		this.requestConfig = RequestConfig.custom()
				.setSocketTimeout(3000).setConnectTimeout(3000).build();
		this.httpClient = (CloseableHttpClient) wrapClient();
		this.httpPost.setConfig(this.requestConfig);
		this.httpGet.setConfig(this.requestConfig);
	}

	/**
	 * @Description 处理https请求的post方法
	 * @param url
	 *            :url
	 * @param params
	 *            :参数
	 * @return 返回的字符串
	 */
	public String httpsPostInvoke(String url, Map<String, String> params) {
		String result = "";
		try {
			this.httpPost.setURI(URI.create(url));
			List<NameValuePair> ps = new ArrayList<NameValuePair>();
			if(params != null && params.size() >0) {
				for (String pKey : params.keySet()) {
					ps.add(new BasicNameValuePair(pKey, params.get(pKey)));
				}
			}
			this.httpPost.setEntity(new UrlEncodedFormEntity(ps, CHARSET));
			CloseableHttpResponse response = this.httpClient.execute(this.httpPost);
			HttpEntity httpEntity = response.getEntity();
			result = EntityUtils.toString(httpEntity, CHARSET);
		} catch (Exception e) {
			e.printStackTrace();
			result = "";
		}finally {
			return result;
		}
	}

	/**
	 * @Description 处理https请求的get方法
	 * @param url
	 *            :url
	 * @param params
	 *            :参数
	 * @return 返回的字符串
	 */
	public String httpsGetInvoke(String url, Map<String, String> params) {
		String result = "";
		try {
			if(params != null && params.size() > 0) {
				String ps = "";
				for (String pKey : params.keySet()) {
					if (!"".equals(ps)) {
						ps = ps + "&";
					}
					// 处理特殊字符，%替换成%25，空格替换为%20，#替换为%23
					String pValue = params.get(pKey).replace("%", "%25")
							.replace(" ", "%20").replace("#", "%23");
					ps += pKey + "=" + pValue;
				}
				if (!"".equals(ps)) {
					url = url + "?" + ps;
				}
			}
			this.httpGet.setURI(URI.create(url));
			CloseableHttpResponse response = httpClient.execute(httpGet);
			HttpEntity httpEntity = response.getEntity();
			result = EntityUtils.toString(httpEntity, CHARSET);
		} catch (Exception e) {
			result = "";
			e.printStackTrace();
		} finally {
			return result;
		}
	}

	/**
	 * 释放连接
	 */
	public void releaseConnection() {
		try {
			if (httpPost != null) {
				httpPost.releaseConnection();
			}
			if (httpClient != null) {
				httpClient.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (httpGet != null) {
					httpGet.releaseConnection();
				}
				if (httpClient != null) {
					httpClient.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * @Description 创建一个不进行正式验证的请求客户端对象 不用导入SSL证书
	 * @return HttpClient
	 */
	private HttpClient wrapClient() {
		try {
			SSLContext ctx = SSLContext.getInstance("TLS");
			X509TrustManager tm = new X509TrustManager() {
				public X509Certificate[] getAcceptedIssuers() {
					return null;
				}

				public void checkClientTrusted(X509Certificate[] arg0,
											   String arg1) throws CertificateException {
				}

				public void checkServerTrusted(X509Certificate[] arg0,
											   String arg1) throws CertificateException {
				}
			};
			ctx.init(null, new TrustManager[] { tm }, null);
			SSLConnectionSocketFactory ssf = new SSLConnectionSocketFactory(
					ctx, null);
			CloseableHttpClient httpclient = HttpClients.custom()
					.setSSLSocketFactory(ssf).build();
			return httpclient;
		} catch (Exception e) {
			e.printStackTrace();
			return HttpClients.createDefault();
		}
	}
}