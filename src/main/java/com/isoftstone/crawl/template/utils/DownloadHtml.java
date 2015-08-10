package com.isoftstone.crawl.template.utils;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.URIException;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;

public class DownloadHtml {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			String url = "http://www.cs.com.cn/xwzx/hg/";
			System.out.println(getHtml(url,"gb2312"));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// System.out.println(new
		// String(getHtml("http://irm.cnstock.com/ivlist/index/yqjj/0")));
	}

	public static String getHtml(String url, String charset) {
		String responseBody = "";
		// 创建Get连接方法的实例
		HttpMethod getMethod = null;
		try {
			// 创建 HttpClient 的实例
			HttpClient httpClient = new HttpClient();
			// 创建Get连接方法的实例
			// getMethod = new GetMethod(url);
			getMethod = new GetMethod(EncodeUtils.formatUrl(url, ""));
			// 使用系统提供的默认的恢复策略
			getMethod.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler());
			// 设置 get 请求超时为 10秒
			getMethod.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, 10000);

			// 执行getMethod
			int status = httpClient.executeMethod(getMethod);
			// System.out.println("status:" + status);
			// 连接返回的状态码
			if (HttpStatus.SC_OK == status) {
				System.out.println("Connection to " + getMethod.getURI() + " Success!");
				// HTTP响应头部信息
				Pattern pattern = Pattern.compile("text/html;[\\s]*charset=(.*)");
				Header[] headers = getMethod.getResponseHeaders();
				for (Header h : headers) {
					if (h.getName().equalsIgnoreCase("Content-Type")) {
						Matcher m = pattern.matcher(h.getValue());
						if (m.find()) {
							charset = m.group(1);
							break;
						}
					}
				}
				// 获取到的内容
				InputStream resStream = getMethod.getResponseBodyAsStream();

				BufferedReader br = new BufferedReader(new InputStreamReader(resStream, charset));
				StringBuffer resBuffer = new StringBuffer();
				char[] chars = new char[4096];
				int length = 0;
				while (0 < (length = br.read(chars))) {
					resBuffer.append(chars, 0, length);
				}
				resStream.close();

				// StringBuilder 线程是不安全的
				// StringBuilder builder = new StringBuilder();
				// char[] chars = new char[4096];
				// int length = 0;
				// while (0 < (length = br.read(chars))) {
				// builder.append(chars, 0, length);
				// }
				// return builder.toString();
				return resBuffer.toString();
			}
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (URIException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// 释放连接
			getMethod.releaseConnection();
		}
		return responseBody;
	}

	public static String getHtmlSource(String url) {
		String html = RedisUtils.getHtmlResult(url);
		if (html == null) {
			// 创建Get连接方法的实例
			HttpMethod getMethod = null;
			try {
				getMethod = new GetMethod(EncodeUtils.formatUrl(url, ""));
				// 创建 HttpClient 的实例
				HttpClient httpClient = new HttpClient();
				// 创建Get连接方法的实例
				// getMethod = new GetMethod(url);
				// 使用系统提供的默认的恢复策略
				getMethod.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler());
				// 设置 get 请求超时为 10秒
				getMethod.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, 10000);
				// 执行getMethod
				int status = httpClient.executeMethod(getMethod);
				System.out.println("status:" + status);
				// 连接返回的状态码
				if (HttpStatus.SC_OK == status) {
					System.out.println("Connection to " + getMethod.getURI() + " Success!");
					// 获取到的内容
					InputStream in = getMethod.getResponseBodyAsStream();

					BufferedReader br = new BufferedReader(new InputStreamReader(in, "utf-8"));
					StringBuffer resBuffer = new StringBuffer();
					char[] chars = new char[4096];
					int length = 0;
					while (0 < (length = br.read(chars))) {
						resBuffer.append(chars, 0, length);
					}
					in.close();
					return resBuffer.toString();
				}
			} catch (UnsupportedEncodingException e1) {
				e1.printStackTrace();
			} catch (URIException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				// 释放连接
				getMethod.releaseConnection();
			}
		} else {
			return html;
		}
		return null;
	}

	public static final byte[] input2byte(InputStream inStream) throws IOException {
		ByteArrayOutputStream swapStream = new ByteArrayOutputStream();
		byte[] buff = new byte[100];
		int rc = 0;
		while ((rc = inStream.read(buff, 0, 100)) > 0) {
			swapStream.write(buff, 0, rc);
		}
		byte[] in2b = swapStream.toByteArray();
		return in2b;
	}

	public static byte[] getHtml(String url) {
		byte[] html = RedisUtils.getHtmlResultByte(url);
		if (html == null) {
			// 创建Get连接方法的实例
			HttpMethod getMethod = null;
			try {
				getMethod = new GetMethod(EncodeUtils.formatUrl(url, ""));
				// 创建 HttpClient 的实例
				HttpClient httpClient = new HttpClient();
				// 创建Get连接方法的实例
				// getMethod = new GetMethod(url);
				// 使用系统提供的默认的恢复策略
				getMethod.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler());
				// 设置 get 请求超时为 10秒
				getMethod.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, 10000);
				// 执行getMethod
				int status = httpClient.executeMethod(getMethod);
				System.out.println("status:" + status);
				// 连接返回的状态码
				if (HttpStatus.SC_OK == status) {
					System.out.println("Connection to " + getMethod.getURI() + " Success!");
					// 获取到的内容
					InputStream in = getMethod.getResponseBodyAsStream();
					html = input2byte(in);
					RedisUtils.setHtmlResult(url, html);
					return html;
				}
			} catch (UnsupportedEncodingException e1) {
				e1.printStackTrace();
			} catch (URIException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				// 释放连接
				getMethod.releaseConnection();
			}
		} else {
			return html;
		}
		return null;
	}
}
