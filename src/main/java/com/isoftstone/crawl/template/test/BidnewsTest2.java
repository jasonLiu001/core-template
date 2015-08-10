package com.isoftstone.crawl.template.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.alibaba.fastjson.JSON;
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

public class BidnewsTest2 {

	public static void main(String[] args) {
		// 全国招标信息网
		// 1、生成模板
		String templateUrl = "http://www.bidnews.cn/news/dianligongsi-38291.html";
		byte[] input = DownloadHtml.getHtml(templateUrl);
		
		TemplateResult templateResult = bidbewsTemplate(templateUrl,input,Constants.DEFAULT_REDIS_DBINDEX);
		// 2、测试列表页
		
		String encoding = "utf-8";
		System.out.println(JSON.toJSONString(templateResult));
		ParseResult parseResult = null;
		// parseResult = TemplateFactory.localProcess(input, encoding,
		// templateUrl, templateResult, Constants.TEMPLATE_LIST);
		parseResult = TemplateFactory.process(input, encoding, templateUrl,Constants.DEFAULT_REDIS_DBINDEX);
		//System.out.println("templateResult:" + templateResult.toJSON());
		System.out.println("List parseResult:"+parseResult.toJSON());
		// System.out.println(TemplateFactory.getOutlink(parseResult).toString());
		// /System.out.println(TemplateFactory.getPaginationOutlink(parseResult).toString());
		// 3、测试内容页
		templateUrl = "http://www.bidnews.cn/news/zhaobiao-219300.html";
		input = DownloadHtml.getHtml(templateUrl);
		encoding = "utf-8";
		parseResult = TemplateFactory.process(input, encoding, templateUrl,Constants.DEFAULT_REDIS_DBINDEX);
//		 parseResult = TemplateFactory.localProcess(input, encoding,
//		 templateUrl, templateResult, Constants.TEMPLATE_NEWS);
		System.out.println("News parseResult:"+parseResult.toJSON());
	}

	public static TemplateResult bidbewsTemplate(String templateUrl,byte[] input,int dbindex) {
		HashMap<String, String> dictionary = new HashMap<String, String>();
		TemplateResult template = new TemplateResult();

		template.setType(Constants.TEMPLATE_LIST);
		dictionary.put("分类", "全国招标信息网");
		dictionary.put("项目", "商机通");
		template.setTags(dictionary);
		
		String templateGuid = MD5Utils.MD5(templateUrl);
		template.setTemplateGuid(templateGuid);
		template.setState(Constants.UN_FETCH);

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
		indexer.initJsoupIndexer("#DataList1 > tbody > tr > td > div > div.sf-listxxbt > ul > li > a:nth-child(2)", Constants.ATTRIBUTE_HREF);
		selector.initContentSelector(indexer, null);
		list.add(selector);
		template.setList(list);

		// pagitation outlink js翻页无法处理
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("#AspNetPager1 > cite", Constants.ATTRIBUTE_TEXT);
		filter = new SelectorFilter();
		filter.initMatchFilter("(\\d+)页");
		selector.initPagitationSelector(Constants.PAGINATION_TYPE_PAGENUMBER, "##", "--", "http://www.bidnews.cn/caigou/gonggao-38046##.html", "2", "", indexer, filter, null);
		pagination.add(selector);
		template.setPagination(pagination);

		// html
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("body > table > tbody > tr > td > table:nth-child(5) > tbody > tr:nth-child(1) > td > table", Constants.ATTRIBUTE_HTML);
		selector.initFieldSelector("page_content", "", indexer, null, null);
		news.add(selector);
		
		// title
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("body > table > tbody > tr > td > table:nth-child(3) > tbody > tr:nth-child(1) > td > h3", Constants.ATTRIBUTE_TEXT);
		selector.initFieldSelector("title", "", indexer, null, null);
		news.add(selector);

		// content
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("body > table > tbody > tr > td > table:nth-child(5) > tbody > tr:nth-child(1) > td > table", Constants.ATTRIBUTE_TEXT);
		selector.initFieldSelector("content", "", indexer, null, null);
		news.add(selector);

		// tstamp
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("body > table > tbody > tr > td > table:nth-child(4) > tbody > tr > td:nth-child(1) > ul > ul > li", Constants.ATTRIBUTE_TEXT);
		filter = new SelectorFilter();
		filter.initMatchFilter(Constants.YYYYMMDD);
		selector.initFieldSelector("tstamp", "", indexer, filter, null);
		news.add(selector);

		template.setNews(news);

		RedisUtils.setTemplateResult(template, templateGuid,dbindex);
		return template;

	}

}
