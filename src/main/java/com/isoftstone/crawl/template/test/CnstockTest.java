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

public class CnstockTest {

	public static void main(String[] args) {
		// 1、生成模板
		String templateUrl = "http://irm.cnstock.com/ivlist/index/yqjj/0";
		TemplateResult templateResult = cnstockTemplate(templateUrl,Constants.DEFAULT_REDIS_DBINDEX);
		// 2、测试列表页
		ParseResult parseResult = null;
		String encoding = "utf-8";
		byte[] input = DownloadHtml.getHtml(templateUrl);
		//parseResult = TemplateFactory.localProcess(input, encoding, url, templateResult, Constants.TEMPLATE_LIST);
		parseResult = TemplateFactory.process(input, encoding, templateUrl,Constants.DEFAULT_REDIS_DBINDEX);
		System.out.println("templateResult:" + templateResult.toJSON());
		System.out.println(parseResult.toJSON());
		// 3、测试内容页
		templateUrl = "http://irm.cnstock.com/company/scp_tzzgx/tgx_yqjj/201411/3250507.htm";
		input = DownloadHtml.getHtml(templateUrl);
		encoding = "gbk";
		parseResult = TemplateFactory.process(input, encoding, templateUrl,Constants.DEFAULT_REDIS_DBINDEX);
		//parseResult = TemplateFactory.localProcess(input, encoding, templateUrl,templateResult, Constants.TEMPLATE_NEWS);
		System.out.println("templateResult:" + templateResult.toJSON());
		System.out.println(parseResult.toJSON());
	}

	public static TemplateResult cnstockTemplate(String templateUrl,int dbindex) {
		TemplateResult template = new TemplateResult();
		template.setType(Constants.TEMPLATE_LIST);
		String templateGuid = MD5Utils.MD5(templateUrl);
		template.setTemplateGuid(templateGuid);
		template.setState(Constants.UN_FETCH);
		
		HashMap<String, String> dictionary = new HashMap<String, String>();
		dictionary.put("分类", "中国证卷网");
		dictionary.put("项目", "企业舆情-信托");
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
		indexer.initJsoupIndexer("body > div.container.video-wrap > div.main-wrap > div.publish-list.mt15 > ul > li > a", Constants.ATTRIBUTE_HREF);
		selector.initContentSelector(indexer, null);
		list.add(selector);
		template.setList(list);

		// pagitation outlink
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("div.pagination.pagination-centered ul li a:contains(末页)", Constants.ATTRIBUTE_HREF);
		filter = new SelectorFilter();
		filter.initMatchFilter("\\d+");
		selector.initPagitationSelector(Constants.PAGINATION_TYPE_PAGENUMBER, "0", "", templateUrl, "1", null, indexer, filter, null);
		pagination.add(selector);
		template.setPagination(pagination);

		// html
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("div#qmt_content_div", Constants.ATTRIBUTE_HTML);
		selector.initFieldSelector("page_content", "", indexer, null, null);
		news.add(selector);
		
		// title
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("body > div.container.video-wrap > div.main-wrap.common-detail-blank > div.main-title > div > h1", Constants.ATTRIBUTE_TEXT);
		selector.initFieldSelector("title", "", indexer, null, null);
		news.add(selector);

		// content
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("div#qmt_content_div", Constants.ATTRIBUTE_TEXT);
		selector.initFieldSelector("content", "", indexer, null, null);
		news.add(selector);

		// tstamp
		selector = new Selector();
		indexer = new SelectorIndexer();
		indexer.initJsoupIndexer("body > div.container.video-wrap > div.main-wrap.common-detail-blank > div.main-title > div > div > span.time", Constants.ATTRIBUTE_TEXT);
		selector.initFieldSelector("tstamp", "", indexer, null, null);
		news.add(selector);
		template.setNews(news);

		RedisUtils.setTemplateResult(template, templateGuid,dbindex);
		return template;
	}
}
