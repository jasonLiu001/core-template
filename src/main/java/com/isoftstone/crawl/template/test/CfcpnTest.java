package com.isoftstone.crawl.template.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.isoftstone.crawl.template.global.Constants;
import com.isoftstone.crawl.template.impl.ParseResult;
import com.isoftstone.crawl.template.impl.Selector;
import com.isoftstone.crawl.template.impl.SelectorFilter;
import com.isoftstone.crawl.template.impl.SelectorFormat;
import com.isoftstone.crawl.template.impl.SelectorIndexer;
import com.isoftstone.crawl.template.impl.TemplateFactory;
import com.isoftstone.crawl.template.impl.TemplateResult;
import com.isoftstone.crawl.template.utils.DownloadHtml;
import com.isoftstone.crawl.template.utils.MD5Utils;
import com.isoftstone.crawl.template.utils.RedisUtils;

public class CfcpnTest {

	public static void main(String[] args) {
		// 金采网
		// 1、生成模板
		String templateUrl = "http://www.cfcpn.com/front/notice/advsearch_list.jsp?offset=0";
		TemplateResult templateResult = cfcpnTemplate(templateUrl,Constants.DEFAULT_REDIS_DBINDEX);
		
		// 2、测试列表页
		String encoding = "gb2312";
		byte[] input = DownloadHtml.getHtml(templateUrl);
		ParseResult parseResult = null;
		//parseResult = TemplateFactory.localProcess(input, encoding, templateUrl, templateResult, Constants.TEMPLATE_LIST);
		 parseResult = TemplateFactory.process(input, encoding, templateUrl,Constants.DEFAULT_REDIS_DBINDEX);
		System.out.println("templateResult:" + templateResult.toJSON());
		System.out.println(parseResult.toJSON());
		// 3、测试内容页
		templateUrl = "http://www.cfcpn.com/front/notice/show_news.jsp?op=browse&f_charge=0&hyid=0&rec_id=242111";
		input = DownloadHtml.getHtml(templateUrl);
		encoding = "gb2312";
		 parseResult = TemplateFactory.process(input, encoding, templateUrl,Constants.DEFAULT_REDIS_DBINDEX);
		 //parseResult = TemplateFactory.localProcess(input, encoding, templateUrl, templateResult, Constants.TEMPLATE_NEWS);
		System.out.println(parseResult.toJSON());
	}
	public static TemplateResult cfcpnTemplate(String templateUrl,int dbindex) {
		TemplateResult template = new TemplateResult();
		template.setType(Constants.TEMPLATE_LIST);
		String templateGuid = MD5Utils.MD5(templateUrl);
		template.setTemplateGuid(templateGuid);
		template.setState(Constants.UN_FETCH);
		
		HashMap<String, String> dictionary = new HashMap<String, String>();
		dictionary.put("分类", "金采网");
		dictionary.put("项目", "商机通");
		template.setTags(dictionary);
		
		List<Selector> list = new ArrayList<Selector>();
		List<Selector> news = new ArrayList<Selector>();
		List<Selector> pagination = new ArrayList<Selector>();
		SelectorIndexer indexer = null;
		Selector selector = null;
		SelectorFilter filter = null;
		SelectorFormat format = null;

		// content outlink
		selector = new Selector();
		indexer = new SelectorIndexer();
		indexer.initJsoupIndexer("#list_table > tbody > tr > td:nth-child(2) > a", Constants.ATTRIBUTE_HREF);
		selector.initContentSelector(indexer, null);
		// city
		Selector label = new Selector();
		label.setType(Constants.SELECTOR_LABEL);
		indexer = new SelectorIndexer();
		indexer.initJsoupIndexer("#list_table > tbody > tr > td:nth-child(4)", Constants.ATTRIBUTE_TEXT);
		label.initLabelSelector("city", "", indexer, null, null);
		selector.setLabel(label);
		list.add(selector);
		template.setList(list);
		
		// pagitation outlink
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("body > div:nth-child(1) > div.auto.top10 > div.w720.left > div.auto.padding10.border_ccc.top10 > div", Constants.ATTRIBUTE_TEXT);
		filter = new SelectorFilter();
		filter.initMatchFilter("共(\\d+)页");
		selector.initPagitationSelector(Constants.PAGINATION_TYPE_PAGENUMBER_INTERVAL, "##", "", "http://www.cfcpn.com/front/notice/advsearch_list.jsp?offset=##", "", "",20, indexer, filter, null);
		pagination.add(selector);
		template.setPagination(pagination);

		// html
		indexer = new SelectorIndexer();
		selector = new Selector();//body > div:nth-child(1) > div > div.w720.left > div.border_ccc.auto.top10.aticle
		indexer.initJsoupIndexer("#content", Constants.ATTRIBUTE_HTML);
		selector.initFieldSelector("page_content", "", indexer, null, null);
		news.add(selector);
		
		// title
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("body > div:nth-child(1) > div > div.w720.left > div.border_ccc.auto.top10.aticle > div:nth-child(1)", Constants.ATTRIBUTE_TEXT);
		selector.initFieldSelector("title", "", indexer, null, null);
		news.add(selector);

		// content
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("#content", Constants.ATTRIBUTE_TEXT);
		selector.initFieldSelector("content", "", indexer, null, null);
		news.add(selector);

		// tstamp
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("body > div:nth-child(1) > div > div.w720.left > div.border_ccc.auto.top10.aticle > div.aticle_time.txtCenter", Constants.ATTRIBUTE_TEXT);
		filter = new SelectorFilter();
		filter.initMatchFilter(Constants.YYYYMMDD);
		selector.initFieldSelector("tstamp", "", indexer, filter, null);
		news.add(selector);

		template.setNews(news);

		RedisUtils.setTemplateResult(template, templateGuid,dbindex);
		return template;

	}

}
