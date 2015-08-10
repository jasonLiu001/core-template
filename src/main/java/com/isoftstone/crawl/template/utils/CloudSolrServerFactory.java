package com.isoftstone.crawl.template.utils;
import java.net.MalformedURLException;

import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.LBHttpSolrServer;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloudSolrServerFactory {
	
	private static final Logger logger = LoggerFactory.getLogger(CloudSolrServerFactory.class);
	private static CloudSolrServerFactory instance;
	private static PropertiesUtils propert = PropertiesUtils.getInstance();
	private static String solr_server_name=propert.getValue("solr_server_name");
	private static String zk_server_host=propert.getValue("zk_server_host");
	private CloudSolrServer cloudSolrServer;
	
	private  CloudSolrServerFactory(){
		try {
			initCloudSolrServer(zk_server_host,solr_server_name);
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	static synchronized public CloudSolrServerFactory getInstance() {
		if (instance == null) {
			instance = new CloudSolrServerFactory();
		}
		return instance;
	}
	/**
	 * 创建SOLR服务连接
	 * 
	 * @param collectionName
	 * @return
	 * @throws MalformedURLException 
	 */
	private void initCloudSolrServer(String zkHosts, String collectionName) throws MalformedURLException {
		
		// 设置请求连接限制
		ModifiableSolrParams params = new ModifiableSolrParams();
		params.set(HttpClientUtil.PROP_MAX_CONNECTIONS, 200);
		params.set(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, 200);
		HttpClient httpClient =  HttpClientUtil.createClient(params);
		
		// LoadBalanced HttpSolrServer
		LBHttpSolrServer lbServer = new LBHttpSolrServer(httpClient);
		cloudSolrServer = new CloudSolrServer(zkHosts, lbServer);
		cloudSolrServer.setZkClientTimeout(20000);
		cloudSolrServer.setZkConnectTimeout(12000);
		cloudSolrServer.setDefaultCollection(collectionName);
		cloudSolrServer.connect();
		logger.info("CloudSolr server initilization successfully!......");
		
	}
	
	public CloudSolrServer getCloudSolrServer()
	{
		return cloudSolrServer;
	}
}