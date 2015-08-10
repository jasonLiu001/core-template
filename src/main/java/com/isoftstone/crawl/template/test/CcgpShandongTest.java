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

public class CcgpShandongTest {

	public static void main(String[] args) {
		// 中国山东政府采购网公告
		// 1、生成模板
		String nextPage = "http://www.ccgp-shandong.gov.cn/fin_info/site/channelall.jsp?curpage=##&colcode=030";
		String templateUrl = "http://www.ccgp-shandong.gov.cn/fin_info/site/channelall.jsp?curpage=1&colcode=030";
		TemplateResult templateResult = null;
		for (int i = 1; i <= 5; i++) {
			templateResult = ccgpTemplate(templateUrl + i, nextPage + i,Constants.DEFAULT_REDIS_DBINDEX);
		}

		// 2、测试列表页
		String encoding = "gb2312";
		byte[] input = null;
		ParseResult parseResult = null;
		templateUrl = "http://www.ccgp-shandong.gov.cn/fin_info/site/channelall.jsp?curpage=1&colcode=0301";
		input = DownloadHtml.getHtml(templateUrl);
		//parseResult = TemplateFactory.localProcess(input, encoding, templateUrl, templateResult, Constants.TEMPLATE_LIST);
		 parseResult = TemplateFactory.process(input, encoding, templateUrl,Constants.DEFAULT_REDIS_DBINDEX);
		System.out.println("templateResult:" + templateResult.toJSON());
		System.out.println(parseResult.toJSON());
		// 3、测试内容页
		templateUrl = "http://www.ccgp-shandong.gov.cn/fin_info/site/read.jsp?colcode=0301&id=171230";
		input = DownloadHtml.getHtml(templateUrl);
		//parseResult = TemplateFactory.localProcess(input, encoding, templateUrl, templateResult, Constants.TEMPLATE_NEWS);
		parseResult = TemplateFactory.process(input, encoding, templateUrl,Constants.DEFAULT_REDIS_DBINDEX);
		System.out.println("templateResult:" + templateResult.toJSON());
		System.out.println(parseResult.toJSON());
	}

	public static TemplateResult ccgpTemplate(String templateUrl, String nextPageUrl,int dbindex) {
		TemplateResult template = new TemplateResult();
		template.setType(Constants.TEMPLATE_LIST);
		String templateGuid = MD5Utils.MD5(templateUrl);
		template.setTemplateGuid(templateGuid);
		template.setState(Constants.UN_FETCH);
		
		HashMap<String, String> dictionary = new HashMap<String, String>();
		dictionary.put("分类", "山东政府采购网");
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
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("body > div:nth-child(4) > table > tbody > tr > td:nth-child(2) > table:nth-child(1) > tbody > tr:nth-child(2) > td:nth-child(2) > table:nth-child(2) > tbody > tr:nth-child(2n-1) > td > a", Constants.ATTRIBUTE_HREF);
		selector.initContentSelector(indexer, null);
		list.add(selector);
		template.setList(list);

		// pagitation outlink js翻页无法处理
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("body > div:nth-child(4) > table > tbody > tr > td:nth-child(2) > table:nth-child(1) > tbody > tr:nth-child(2) > td:nth-child(2) > table:nth-child(3) > tbody > tr > td > strong", Constants.ATTRIBUTE_TEXT);
		filter = new SelectorFilter();
		filter.initMatchFilter("/(\\d+)");
		selector.initPagitationSelector(Constants.PAGINATION_TYPE_PAGENUMBER, "##", "", nextPageUrl, "2", null, indexer, filter, null);
		pagination.add(selector);
		template.setPagination(pagination);

		// html
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("body > div:nth-child(3) > table > tbody > tr > td:nth-child(2) > table:nth-child(1) > tbody > tr:nth-child(2) > td:nth-child(2) > table:nth-child(2) > tbody", Constants.ATTRIBUTE_HTML);
		selector.initFieldSelector("page_content", "", indexer, null, null);
		news.add(selector);
		
		// title
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("body > div:nth-child(3) > table > tbody > tr > td:nth-child(2) > table:nth-child(1) > tbody > tr:nth-child(2) > td:nth-child(2) > table:nth-child(2) > tbody > tr:nth-child(1) > td > b", Constants.ATTRIBUTE_TEXT);
		selector.initFieldSelector("title", "", indexer, null, null);
		news.add(selector);

		// content
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("body > div:nth-child(3) > table > tbody > tr > td:nth-child(2) > table:nth-child(1) > tbody > tr:nth-child(2) > td:nth-child(2) > table:nth-child(2) > tbody", Constants.ATTRIBUTE_TEXT);
		selector.initFieldSelector("content", "", indexer, null, null);
		news.add(selector);

		// tstamp
		selector = new Selector();
		indexer = new SelectorIndexer();
		indexer.initJsoupIndexer("body > div:nth-child(3) > table > tbody > tr > td:nth-child(2) > table:nth-child(1) > tbody > tr:nth-child(2) > td:nth-child(2) > table:nth-child(2) > tbody > tr:nth-last-child(3) > td", Constants.ATTRIBUTE_TEXT);
		filter = new SelectorFilter();
		filter.initMatchFilter(Constants.YYYYMMDD);
		selector.initFieldSelector("tstamp", "", indexer, filter, null);
		news.add(selector);
		template.setNews(news);

		RedisUtils.setTemplateResult(template, templateGuid,dbindex);
		return template;

	}
}
