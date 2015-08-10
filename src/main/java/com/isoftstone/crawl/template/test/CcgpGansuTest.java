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

public class CcgpGansuTest {

	public static void main(String[] args) {
		int[] number = { 138, 140, 141, 220, 213, 214, 215, 216 };
		// 1、生成模板
		String nextPage = "http://www.ccgp-gansu.gov.cn/votoonadmin/article/classlist.jsp?pn=##&class_id=";
		String templateUrl = "http://www.ccgp-gansu.gov.cn/votoonadmin/article/classlist.jsp?pn=1&class_id=";
		String encoding = "gb2312";
		TemplateResult templateResult = null;
		ParseResult parseResult = null;

		for (int i = 0; i < number.length; i++) {
			int n = number[i];
			templateResult = ccgpGansuTemplate(templateUrl + n, nextPage + n,Constants.DEFAULT_REDIS_DBINDEX);
			System.out.println(templateResult);
		}
		// 2、测试列表页
		templateUrl = "http://www.ccgp-gansu.gov.cn/votoonadmin/article/classlist.jsp?pn=1&class_id=214";
		byte[] input = DownloadHtml.getHtml(templateUrl);
		//parseResult = TemplateFactory.localProcess(input, encoding, templateUrl, templateResult, Constants.TEMPLATE_LIST);
		parseResult = TemplateFactory.process(input, encoding, templateUrl,Constants.DEFAULT_REDIS_DBINDEX);
		System.out.println("templateResult:" + templateResult.toJSON());
		System.out.println(parseResult.toJSON());
		// 3、测试内容页
		templateUrl = "http://www.ccgp-gansu.gov.cn/web/214/228481.html";
		input = DownloadHtml.getHtml(templateUrl);
		parseResult = TemplateFactory.process(input, encoding, templateUrl,Constants.DEFAULT_REDIS_DBINDEX);
		 //parseResult = TemplateFactory.localProcess(input, encoding, templateUrl, templateResult, Constants.TEMPLATE_NEWS);
		System.out.println(parseResult.toJSON());

	}

	public static TemplateResult ccgpGansuTemplate(String templateUrl, String nextPageUrl,int dbindex) {
		TemplateResult template = new TemplateResult();
		template.setType(Constants.TEMPLATE_LIST);
		HashMap<String, String> dictionary = new HashMap<String, String>();
		dictionary.put("分类", "甘肃政府采购网");
		dictionary.put("项目", "商机通");
		String templateGuid = MD5Utils.MD5(templateUrl);
		template.setTemplateGuid(templateGuid);
		template.setTags(dictionary);
		template.setState(Constants.UN_FETCH);
		
		List<Selector> list = new ArrayList<Selector>();
		List<Selector> news = new ArrayList<Selector>();
		List<Selector> pagination = new ArrayList<Selector>();
		SelectorIndexer indexer = null;
		Selector selector = null;
		SelectorFilter filter = null;
		SelectorFormat format = null;

		// content outlink
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("body > form > table:nth-child(2) > tbody > tr > td:nth-child(2) > table:nth-child(2) > tbody > tr > td > table > tbody > tr > td > table:nth-child(2) > tbody > tr:nth-child(1) > td:nth-child(2) > table > tbody > tr:nth-child(2n-1) > td:nth-child(2) > a", Constants.ATTRIBUTE_HREF);
		selector.initContentSelector(indexer, null);

		// tstamp
		Selector label = new Selector();
		label.setType(Constants.SELECTOR_LABEL);
		indexer = new SelectorIndexer();
		indexer.initJsoupIndexer("body > form > table:nth-child(2) > tbody > tr > td:nth-child(2) > table:nth-child(2) > tbody > tr > td > table > tbody > tr > td > table:nth-child(2) > tbody > tr:nth-child(1) > td:nth-child(2) > table > tbody > tr:nth-child(2n-1) > td:nth-child(3)", Constants.ATTRIBUTE_TEXT);
		filter = new SelectorFilter();
		filter.initMatchFilter(Constants.YYYYMMDD);
		label.initLabelSelector("tstamp", "", indexer, filter, null);
		selector.setLabel(label);
		list.add(selector);
		template.setList(list);

		// pagitation outlink js翻页无法处理
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("body > form > table:nth-child(2) > tbody > tr > td:nth-child(2) > table:nth-child(2) > tbody > tr > td > table > tbody > tr > td > table:nth-child(3) > tbody > tr > td:nth-child(1) > font:nth-child(2)", Constants.ATTRIBUTE_TEXT);
		filter = new SelectorFilter();
		filter.initMatchFilter("\\d+");
		selector.initPagitationSelector(Constants.PAGINATION_TYPE_PAGENUMBER, "##", "", nextPageUrl, "2", null, indexer, filter, null);
		pagination.add(selector);
		template.setPagination(pagination);

		
		// page_content
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("body > table:nth-child(2) > tbody > tr > td > table > tbody > tr > td > table > tbody > tr > td > table > tbody > tr > td > table:nth-child(2) > tbody > tr > td > div", Constants.ATTRIBUTE_HTML);
		selector.initFieldSelector("page_content", "", indexer, null, null);
		news.add(selector);
						
		// title
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("body > table:nth-child(2) > tbody > tr > td > table > tbody > tr > td > table > tbody > tr > td > table > tbody > tr > td > table:nth-child(1) > tbody > tr > td", Constants.ATTRIBUTE_TEXT);
		selector.initFieldSelector("title", "", indexer, null, null);
		news.add(selector);

		// content
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("body > table:nth-child(2) > tbody > tr > td > table > tbody > tr > td > table > tbody > tr > td > table > tbody > tr > td > table:nth-child(2) > tbody > tr > td", Constants.ATTRIBUTE_TEXT);
		selector.initFieldSelector("content", "", indexer, null, null);
		news.add(selector);
		template.setNews(news);

		RedisUtils.setTemplateResult(template, templateGuid,dbindex);
		return template;

	}
}
