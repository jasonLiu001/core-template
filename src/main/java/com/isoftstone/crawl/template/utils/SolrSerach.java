package com.isoftstone.crawl.template.utils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.Group;
import org.apache.solr.client.solrj.response.GroupCommand;
import org.apache.solr.client.solrj.response.GroupResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.GroupParams;

/**
 * 
 * @ClassName: SolrSerach
 * @Description: TODO(这里用一句话描述这个类的作用)
 * @author lj
 * @date 2014年8月19日 上午11:38:22
 * 
 */
public class SolrSerach {
	
	
	//private CommonsHttpSolrServer solr = BasicSolrServer.getInstance().getSolrServer();
	private String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";

	/**
	 * @Title: getQueryResultCount
	 * @Description: TODO(返回指定查询条件的数据个数)
	 * @param @param field
	 * @param @param value
	 * @param @return 设定文件
	 * @return long 返回类型
	 * @author lj
	 * @throws
	 */
	public long getQueryResultCount(String field, String value) {
		if (field.isEmpty() || field == null || value.isEmpty() || value == null) {
			return -1;
		}
		
		SolrQuery query = null;
		query = new SolrQuery();
		query.setQuery(field + ":" + value);
		query.addFilterQuery("-tags:dataSource\\:2");
		query.setRows(10);
		QueryResponse rsp = null;
		try {
			rsp = CloudSolrServerFactory.getInstance().getCloudSolrServer().query(query);
			if (rsp != null) {
				return rsp.getResults().getNumFound();
			}
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
		return -1;
	}

	/**
	 * @Title: getQueryResultCount
	 * @Description: TODO(返回指定查询条件和指定日期的数据个数)
	 * @param @param field
	 * @param @param value
	 * @param @return 设定文件
	 * @return long 返回类型
	 * @author lj
	 * @throws
	 */
	public long getQueryResultCount(String field, String value, String filter, Date start, Date end) {
		if (field.isEmpty() || field == null || value.isEmpty() || value == null) {
			return -1;
		}
		SimpleDateFormat df = new SimpleDateFormat(format);
		SolrQuery query = null;
		query = new SolrQuery();
		query.setQuery(field + ":" + value);
		query.addFilterQuery("-tags:dataSource\\:2");
		query.addFilterQuery(filter + ":[" + df.format(DateFormatUtils.nHourBefore(start, 8)) + " TO " + df.format(DateFormatUtils.nHourBefore(end, 8)) + "]");
		query.setRows(10);
		QueryResponse rsp = null;
		try {
			rsp = CloudSolrServerFactory.getInstance().getCloudSolrServer().query(query);
			if (rsp != null) {
				return rsp.getResults().getNumFound();
			}
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
		return -1;
	}

	public HashMap<String, Long> getQueryResultCount(List<String> hosts, String filter, Date start, Date end) {
		HashMap<String, Long> hashmap = new HashMap<String, Long>();
		if (hosts != null) {
			for (String host : hosts) {
				hashmap.put(host, getQueryResultCount("host", host, filter, start, end));
			}
		}
		return hashmap;
	}

	public HashMap<String, Long> getQueryResultCount(List<String> hosts) {
		HashMap<String, Long> hashmap = new HashMap<String, Long>();
		if (hosts != null) {
			for (String host : hosts) {
				hashmap.put(host, getQueryResultCount("host", host));
			}
		}
		return hashmap;
	}

	// /**
	// * @Title: getHostList
	// * @Description: TODO(返回所有host)
	// * @param @return 设定文件
	// * @return List<String> 返回类型
	// * @author lj
	// * @throws
	// */
	// public List<String> getHostList() {
	// List<String> lsHost = new ArrayList<String>();
	// SolrQuery query = null;
	// try {
	// String filters = propert.getValue("filter.value");
	// query = new SolrQuery();
	// query.setQuery("*:*");// 如果没有查询语句，必须这么写，否则会报异常
	// query.setFacet(true);// 是否分组查询
	// query.setRows(0);// 设置返回结果条数，如果你时分组查询，你就设置为0
	// query.addFacetField("host");// 增加分组字段
	// query.setFacetLimit(-1);// 限制每次返回结果数
	//
	// if (filters.contains(",")) {
	// for (String filter : filters.split(",")) {
	// query.addFilterQuery("-tags:" + filter);
	// }
	// } else {
	// query.addFilterQuery("-tags:" + filters);
	// }
	//
	// QueryResponse rsp = null;
	// rsp = solr.query(query);
	// if (rsp != null) {
	// List<Count> lsc = rsp.getFacetField("host").getValues();
	// // int query_count;
	// for (Count count : lsc) {
	// // query_count = (int) count.getCount();
	// // if (query_count >= 100) {
	// // lsHost.add(count.getName());
	// // }
	// lsHost.add(count.getName());
	// }
	// }
	// } catch (Exception e) {
	// e.printStackTrace();
	// return null;
	// }
	// return lsHost;
	// }

	/**
	 * @Title: getHostList
	 * @Description: TODO(返回所有host)
	 * @param @return 设定文件
	 * @return List<String> 返回类型
	 * @author lj
	 * @throws
	 */
	public List<String> getHostList() {
		List<String> lsHost = new ArrayList<String>();
		SolrQuery query = null;
		try {
			query = new SolrQuery();
			query.setQuery("*:*");// 如果没有查询语句，必须这么写，否则会报异常
			query.setParam(GroupParams.GROUP, "true");
			query.setParam(GroupParams.GROUP_FIELD, "host");
			query.setParam(GroupParams.GROUP_LIMIT, "0");
			query.setRows(-1);// 设置返回结果条数，如果你时分组查询，你就设置为0
			query.addFilterQuery("-tags:dataSource\\:2");

			QueryResponse rsp = null;
			rsp = CloudSolrServerFactory.getInstance().getCloudSolrServer().query(query);
			GroupResponse groupResponse = rsp.getGroupResponse();
			if (groupResponse != null) {
				List<GroupCommand> groupList = groupResponse.getValues();
				for (GroupCommand groupCommand : groupList) {
					List<Group> groups = groupCommand.getValues();
					for (Group group : groups) {
						if (group.getGroupValue() != null) {
							lsHost.add(group.getGroupValue());
							// System.out.println((group.getGroupValue()+","+(int)group.getResult().getNumFound()));
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return lsHost;
	}

	public static void main(String[] args) {
		SolrSerach search = new SolrSerach();
		// System.out.println(search.getQueryResultCount("host",
		// "news.163.com"));
		System.out.println(search.getHostList());
		// System.out.println(search.getHostList().size());

		Date date = new Date();
		date.setHours(0);
		System.out.println(search.getQueryResultCount("host", "www.it168.com", "publish_time", date, new Date()));
	}
}
