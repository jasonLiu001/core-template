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

public class GseiTest {

	public static void main(String[] args) {
		// 甘肃经济信息网
		List<String> seeds = new ArrayList<String>();
		seeds.add("http://www.gsei.com.cn/Html/zbgg/zbgg/zbgg/index.html");
		seeds.add("http://www.gsei.com.cn/Html/zbgg/zbgg/zbgs/index.html");
		// 1、生成模板

		TemplateResult templateResult = null;
		for(String s : seeds)
		{
			templateResult = gseiTemplate(s,Constants.DEFAULT_REDIS_DBINDEX);
		}
		// 2、测试列表页
		String templateUrl = "http://www.gsei.com.cn/Html/zbgg/zbgg/zbgg/index.html";
		byte[] input = DownloadHtml.getHtml(templateUrl);
		String encoding = "gb2312";
		ParseResult parseResult = null;
		// parseResult = TemplateFactory.localProcess(input, encoding,
		// templateUrl, templateResult, Constants.TEMPLATE_LIST);
		parseResult = TemplateFactory.process(input, encoding, templateUrl,Constants.DEFAULT_REDIS_DBINDEX);
		System.out.println("templateResult:" + templateResult.toJSON());
		System.out.println(parseResult.toJSON());

		// 3、测试内容页
		templateUrl = "http://www.gsei.com.cn/Html/zbgg/zbgg/zbgg_2015-01-05/20477985.html";
		input = DownloadHtml.getHtml(templateUrl);
		encoding = "gb2312";
		parseResult = TemplateFactory.process(input, encoding, templateUrl,Constants.DEFAULT_REDIS_DBINDEX);
		// parseResult = TemplateFactory.localProcess(input, encoding,
		// templateUrl, templateResult, Constants.TEMPLATE_NEWS);
		System.out.println(parseResult.toJSON());
	}

	public static TemplateResult gseiTemplate(String templateUrl,int dbindex) {
		TemplateResult template = new TemplateResult();
		template.setType(Constants.TEMPLATE_LIST);
		
		HashMap<String, String> dictionary = new HashMap<String, String>();
		dictionary.put("分类", "甘肃经济信息网");
		dictionary.put("项目", "商机通");
		
		String templateGuid = MD5Utils.MD5(templateUrl);
		template.setTemplateGuid(templateGuid);
		template.setState(Constants.UN_FETCH);
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
		indexer.initJsoupIndexer("body > div:nth-child(4) > table > tbody > tr > td:nth-child(2) > table > tbody > tr:nth-child(3) > td > table > tbody > tr > td > table > tbody > tr > td > table > tbody > tr > td > table > tbody > tr:nth-child(2n-1) > td:nth-child(1) > a", Constants.ATTRIBUTE_HREF);
		selector.initContentSelector(indexer, null);
		list.add(selector);
		template.setList(list);

		// pagitation outlink js翻页无法处理
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("#totalpage", Constants.ATTRIBUTE_TEXT);
		filter = new SelectorFilter();
		filter.initMatchFilter("\\d+");
		selector.initPagitationSelector(Constants.PAGINATION_TYPE_PAGENUMBER, "index", "index_", "http://www.gsei.com.cn/Html/zbgg/zbgg/zbgg/index.html", "2", "", indexer, filter, null);
		pagination.add(selector);
		template.setPagination(pagination);

		// html
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("body > table > tbody > tr > td:nth-child(2) > table > tbody > tr:nth-child(3) > td > table > tbody > tr > td > table > tbody > tr > td > table > tbody > tr:nth-child(2) > td", Constants.ATTRIBUTE_HTML);
		selector.initFieldSelector("page_content", "", indexer, null, null);
		news.add(selector);

		// title
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("body > table > tbody > tr > td:nth-child(2) > table > tbody > tr:nth-child(2) > td > table > tbody > tr > td > table > tbody > tr > td", Constants.ATTRIBUTE_TEXT);
		selector.initFieldSelector("title", "", indexer, null, null);
		news.add(selector);

		// content
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("body > table > tbody > tr > td:nth-child(2) > table > tbody > tr:nth-child(3) > td > table > tbody > tr > td > table > tbody > tr > td > table > tbody > tr:nth-child(2) > td", Constants.ATTRIBUTE_TEXT);
		selector.initFieldSelector("content", "", indexer, null, null);
		news.add(selector);

		// tstamp
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("body > table > tbody > tr > td:nth-child(2) > table > tbody > tr:nth-child(3) > td > table > tbody > tr > td > table > tbody > tr > td > table > tbody > tr:nth-child(1) > td", Constants.ATTRIBUTE_TEXT);
		filter = new SelectorFilter();
		filter.initMatchFilter(Constants.YYYYMMDD);
		selector.initFieldSelector("tstamp", "", indexer, filter, null);
		news.add(selector);

		template.setNews(news);

		RedisUtils.setTemplateResult(template, templateGuid,dbindex);
		return template;

	}
}
