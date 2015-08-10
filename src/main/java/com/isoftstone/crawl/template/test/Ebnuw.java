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

public class Ebnuw {

	public static void main(String[] args) {
		String url = "http://www.ebnew.com/tradingIndex.view?key=&pubDateBegin=&pubDateEnd=&infoType=&fundSourceCodes=&zone=&normIndustry=&bidModel=&timeType=&sortMethod=&currentPage=1&length=40";
		String encoding = "utf-8";
		// String html =DownloadHtml.getHtml(url, encoding);
		// System.out.println(html);
		byte[] input = DownloadHtml.getHtml(url);
		TemplateResult templateResult = ebnuwTemplate();
		ParseResult parseResult = null;
		//parseResult = TemplateFactory.localProcess(input, encoding, url, templateResult, Constants.TEMPLATE_LIST);
		parseResult = TemplateFactory.process(input, encoding, url,Constants.DEFAULT_REDIS_DBINDEX);
		System.out.println("templateResult:" + templateResult.toJSON());
		System.out.println(parseResult.toJSON());
		// System.out.println(TemplateFactory.getOutlink(parseResult).toString());
		///System.out.println(TemplateFactory.getPaginationOutlink(parseResult).toString());
		url = "http://www.ebnew.com/businessShow-v-id-498802107.html";
			   
		input = DownloadHtml.getHtml(url);
		encoding = "utf-8";
		parseResult = TemplateFactory.process(input, encoding, url,Constants.DEFAULT_REDIS_DBINDEX);
		//parseResult = TemplateFactory.localProcess(input, encoding, url, templateResult, Constants.TEMPLATE_NEWS);
		System.out.println(parseResult.toJSON());

	}

	public static TemplateResult ebnuwTemplate() {
		TemplateResult template = new TemplateResult();
		template.setType(Constants.TEMPLATE_LIST);
		String templateUrl = "http://www.ebnew.com/tradingIndex.view?key=&pubDateBegin=&pubDateEnd=&infoType=&fundSourceCodes=&zone=&normIndustry=&bidModel=&timeType=&sortMethod=&currentPage=1&length=40";
		String templateGuid = MD5Utils.MD5(templateUrl);
		template.setTemplateGuid(templateGuid);
		template.setState(Constants.UN_FETCH);
		
		HashMap<String, String> dictionary = new HashMap<String, String>();
		dictionary.put("分类", "必联网");
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
		indexer.initJsoupIndexer("body > div.sj_content > div.left_box.sj-result > div > div > p.data_main > span.tit > a", Constants.ATTRIBUTE_HREF);
		selector.initContentSelector(indexer, null);
		
		// 所属行业
		Selector lbtrade = new Selector();
		lbtrade.setType(Constants.SELECTOR_LABEL);
		indexer = new SelectorIndexer();
		indexer.initJsoupIndexer("body > div.sj_content > div.left_box.sj-result > div > div > p.subjoin > span.sshy > i > a", Constants.ATTRIBUTE_TEXT);
		lbtrade.initLabelSelector("trade", "", indexer, null, null);
		selector.setLabel(lbtrade);
		// 项目地区
		Selector lbarea = new Selector();
		lbarea.setType(Constants.SELECTOR_LABEL);
		indexer = new SelectorIndexer();
		indexer.initJsoupIndexer("body > div.sj_content > div.left_box.sj-result > div > div > p.subjoin > span:nth-child(2) > i > a", Constants.ATTRIBUTE_TEXT);
		lbarea.initLabelSelector("area", "", indexer, null, null);
		selector.setLabel(lbarea);
		// tstamp
		Selector lbtstamp = new Selector();
		lbtstamp.setType(Constants.SELECTOR_LABEL);
		indexer = new SelectorIndexer();
		indexer.initJsoupIndexer("body > div.sj_content > div.left_box.sj-result > div > div > p.data_main > span.float_rt", Constants.ATTRIBUTE_TEXT);
		filter = new SelectorFilter();
		filter.initMatchFilter(Constants.YYYYMMDD);
		lbtstamp.initLabelSelector("tstamp", "", indexer, filter, null);
		selector.setLabel(lbtstamp);
		
		list.add(selector);
		template.setList(list);

		// pagitation outlink js翻页无法处理
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("body > div.sj-search > div.sj-search-jg > div.coun.float_lt > span > strong", Constants.ATTRIBUTE_TEXT);
		filter = new SelectorFilter();
		filter.initMatchFilter("\\d+");
		selector.initPagitationSelector(Constants.PAGINATION_TYPE_PAGERECORD, "##", "", "http://www.ebnew.com/tradingIndex.view?key=&pubDateBegin=&pubDateEnd=&infoType=&fundSourceCodes=&zone=&normIndustry=&bidModel=&timeType=&sortMethod=&currentPage=##&length=40", "2", "40", indexer, filter, null);
		pagination.add(selector);
		template.setPagination(pagination);

		// html
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("#mainarea > div:nth-child(1) > div.mbox.bder > div:nth-child(3)", Constants.ATTRIBUTE_HTML);
		selector.initFieldSelector("page_content", "", indexer, null, null);
		news.add(selector);
				
		// title
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("#mainarea > div:nth-child(1) > div.titlehead > h1", Constants.ATTRIBUTE_TEXT);
		selector.initFieldSelector("title", "", indexer, null, null);
		news.add(selector);

		// content
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("#mainarea > div:nth-child(1) > div.mbox.bder > div:nth-child(3)", Constants.ATTRIBUTE_TEXT);
		selector.initFieldSelector("content", "", indexer, null, null);
		news.add(selector);

		template.setNews(news);

		RedisUtils.setTemplateResult(template, templateGuid,0);
		return template;

	}
}
