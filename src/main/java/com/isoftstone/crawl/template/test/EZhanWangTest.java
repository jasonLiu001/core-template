package com.isoftstone.crawl.template.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import com.isoftstone.crawl.template.global.Constants;
import com.isoftstone.crawl.template.impl.ParseResult;
import com.isoftstone.crawl.template.impl.Selector;
import com.isoftstone.crawl.template.impl.SelectorFilter;
import com.isoftstone.crawl.template.impl.SelectorFormat;
import com.isoftstone.crawl.template.impl.SelectorIndexer;
import com.isoftstone.crawl.template.impl.TemplateFactory;
import com.isoftstone.crawl.template.impl.TemplateResult;
import com.isoftstone.crawl.template.utils.MD5Utils;

public class EZhanWangTest {
	public static void main(String[] args) {
		String url = "http://www.eshow365.com/ZhanHui/Ajax/AjaxSearcher.aspx?1=1&starttime=19000101&tag=0&page=1";
		//http://www.eshow365.com/zhanhui/gnzhj.aspx?id=aAreaAdd1&pageIndex=1
		String filePath = "D:/Develop/resource/AjaxSearcher.aspx.html";
		String encoding = "utf8";
		byte[] input = readTextFile(filePath, encoding);
		TemplateResult templateResult = eZhanWangTemplate();
		System.out.println(templateResult.toJSON());
		ParseResult parseResult = TemplateFactory.localProcess(input, encoding, url, templateResult, Constants.TEMPLATE_LIST);
		// System.out.println(parseResult.toJSON());
		System.out.println(TemplateFactory.getOutlink(parseResult).toString());
	}

	public static TemplateResult eZhanWangTemplate() {
		TemplateResult template = new TemplateResult();
		template.setType(Constants.TEMPLATE_LIST);
		String templateUrl = "http://www.eshow365.com/ZhanHui/Ajax/AjaxSearcher.aspx?1=1&starttime=19000101&tag=0&page=1";
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
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("div.sbody dl dt a", Constants.ATTRIBUTE_HREF);
		selector.initContentSelector(indexer, null);
		list.add(selector);
		template.setList(list);

		// pagitation outlink
		indexer = new SelectorIndexer();
		selector = new Selector();
		filter = new SelectorFilter();
		indexer.initJsoupIndexer("div.searchpage ul#pagestr li.recount", Constants.ATTRIBUTE_TEXT);

		selector.initPagitationSelector(Constants.PAGINATION_TYPE_PAGERECORD, "index", "index_", "http://www.zhongguoxintuo.com/xtxw/index.html", "2", "23", indexer, null, null);
		pagination.add(selector);
		template.setPagination(pagination);

		// title
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("div.zhmaincontent h1", Constants.ATTRIBUTE_TEXT);
		selector.initFieldSelector("title", "", indexer, null, null);
		news.add(selector);

		// oriName
		selector = new Selector();
		indexer = null;
		selector.initFieldSelector("oriName", "E展网", indexer, null, null);
		news.add(selector);

		// article_url

		// mainBodyStr
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("div.zhxxcontent", Constants.ATTRIBUTE_TEXT);
		selector.initFieldSelector("mainBodyStr", "", indexer, null, null);
		indexer = new SelectorIndexer();
		indexer.initJsoupIndexer("div.zhleft", Constants.ATTRIBUTE_TEXT);
		selector.setIndexer(indexer);
		news.add(selector);

		// mainBodyHtml
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("div.zhxxcontent", Constants.ATTRIBUTE_HTML);
		selector.initFieldSelector("mainBodyHtml", "", indexer, null, null);
		indexer = new SelectorIndexer();
		indexer.initJsoupIndexer("div.zhleft", Constants.ATTRIBUTE_HTML);
		selector.setIndexer(indexer);
		news.add(selector);

		// area
		selector = new Selector();
		indexer = new SelectorIndexer();
		indexer.initJsoupIndexer("div.zhmaincontent div.zhxxcontent p:eq(3)", Constants.ATTRIBUTE_TEXT);
		selector.initFieldSelector("area", "", indexer, null, null);
		news.add(selector);

		// industry;
		selector = new Selector();
		indexer = new SelectorIndexer();
		indexer.initJsoupIndexer("div.zhmaincontent div.zhxxcontent p:eq(2) a:eq(0)", Constants.ATTRIBUTE_TEXT);
		selector.initFieldSelector("industry", "", indexer, null, null);
		news.add(selector);

		// pavilion
		selector = new Selector();
		indexer = new SelectorIndexer();
		indexer.initJsoupIndexer("div.zhmaincontent div.zhxxcontent a:eq(0)", Constants.ATTRIBUTE_TEXT);
		selector.initFieldSelector("pavilion", "", indexer, null, null);
		news.add(selector);

		// startDate
		selector = new Selector();
		indexer = new SelectorIndexer();
		filter = new SelectorFilter();
		filter.initRemoveFilter("举办时间：");
		format = new SelectorFormat();
		format.initDateFormat("yyyy/MM/dd");
		indexer.initJsoupIndexer("div.zhmaincontent div.zhxxcontent p:eq(0)", Constants.ATTRIBUTE_TEXT);
		selector.initFieldSelector("startDate", "", indexer, filter, format);
		filter = new SelectorFilter();
		filter.initMatchFilter("(\\d{4}/\\d{2}/\\d{2})-");
		selector.setFilter(filter);
		news.add(selector);

		// endDate
		selector = new Selector();
		indexer = new SelectorIndexer();
		filter = new SelectorFilter();
		filter.initRemoveFilter("举办时间：");
		format = new SelectorFormat();
		format.initDateFormat("yyyy/MM/dd");
		indexer.initJsoupIndexer("div.zhmaincontent div.zhxxcontent p:eq(0)", Constants.ATTRIBUTE_TEXT);
		selector.initFieldSelector("endDate", "", indexer, filter, format);
		filter = new SelectorFilter();
		filter.initMatchFilter("-(\\d{4}/\\d{2}/\\d{2})");
		selector.setFilter(filter);
		news.add(selector);
		template.setNews(news);
		return template;
	}

	public static byte[] readTextFile(String filePath, String encoding) {
		java.io.InputStreamReader inputReader = null;
		java.io.BufferedReader reader = null;
		File f = new File(filePath);
		try {
			inputReader = new java.io.InputStreamReader(new java.io.FileInputStream(f), encoding);
			reader = new java.io.BufferedReader(inputReader);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String s = null;
		try {
			s = reader.readLine();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		StringBuffer sb = new StringBuffer();
		while (s != null) {
			sb.append(s);
			sb.append("\r\n");
			try {
				s = reader.readLine();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		byte[] page = sb.toString().getBytes();
		// System.out.println(sb.toString());
		try {
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return page;
	}
}
