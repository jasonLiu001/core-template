package com.isoftstone.crawl.template.utils;

import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpSolrServerFactory {
	private static final Logger logger = LoggerFactory.getLogger(HttpSolrServerFactory.class);
	private static PropertiesUtils propert = PropertiesUtils.getInstance();
	private static String solr_server_url=propert.getValue("solr_server_url");
	private static HttpSolrServerFactory instance;
	private HttpSolrServer httpSolrServe; 
	
	private  HttpSolrServerFactory(){
		initHttpSolrServer(solr_server_url);
	}
	static synchronized public HttpSolrServerFactory getInstance() {
		if (instance == null) {
			instance = new HttpSolrServerFactory();
		}
		return instance;
	}
	
	private void initHttpSolrServer(String solrUri) {
		// 设置请求连接限制
		ModifiableSolrParams params = new ModifiableSolrParams();
		params.set(HttpClientUtil.PROP_MAX_CONNECTIONS, 500);
		params.set(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, 200);
		HttpClient httpClient = HttpClientUtil.createClient(params);

		httpSolrServe = new HttpSolrServer(solrUri, httpClient);
		logger.info("Connected HttpSolrServer Successfully!......");
	}
	public HttpSolrServer getHttpSolrServer(){
		return httpSolrServe;
	}
	
}
