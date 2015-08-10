package com.isoftstone.crawl.template.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
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

public class GuoyuanTest {
	public static void main(String[] args) {
		// 1、生成模板
		String templateUrl = "http://www.zhongguoxintuo.com/xtxw/index.html";
		TemplateResult templateResult = guanyuanTemplate(templateUrl,Constants.DEFAULT_REDIS_DBINDEX);
		System.out.println(templateResult.toJSON());
		// 2、测试列表页
		String encoding = "gb2312";
		ParseResult parseResult =null;
		byte[] input = DownloadHtml.getHtml(templateUrl);
		//parseResult = TemplateFactory.localProcess(input, encoding, templateUrl, templateResult, Constants.TEMPLATE_LIST);
		parseResult = TemplateFactory.process(input, encoding, templateUrl,Constants.DEFAULT_REDIS_DBINDEX);
		System.out.println(parseResult.toJSON());
		// 3、测试内容页
		templateUrl = "http://www.zhongguoxintuo.com/xtxw/5977.html";
		input = DownloadHtml.getHtml(templateUrl);
		//parseResult = TemplateFactory.localProcess(input, encoding, templateUrl, templateResult, Constants.TEMPLATE_NEWS);
		parseResult = TemplateFactory.process(input, encoding, templateUrl,Constants.DEFAULT_REDIS_DBINDEX);
		System.out.println(parseResult.toJSON());
	}

	public static TemplateResult guanyuanTemplate(String templateUrl,int dbindex) {
		TemplateResult template = new TemplateResult();
		template.setType(Constants.TEMPLATE_LIST);

		String templateGuid = MD5Utils.MD5(templateUrl);
		template.setTemplateGuid(templateGuid);
		template.setState(Constants.UN_FETCH);
		
		HashMap<String, String> dictionary = new HashMap<String, String>();
		dictionary.put("分类", "中国信托网");
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
		indexer.initJsoupIndexer("body > div.main > div.main_left > div > div > dl > dt > a", Constants.ATTRIBUTE_HREF);
		selector.initContentSelector(indexer, null);
		list.add(selector);
		template.setList(list);

		// pagitation outlink
		indexer = new SelectorIndexer();
		selector = new Selector();
		filter = new SelectorFilter();
		indexer.initJsoupIndexer("div.page a:nth-child(1) b:nth-child(1)", Constants.ATTRIBUTE_TEXT);
		selector.initPagitationSelector(Constants.PAGINATION_TYPE_PAGERECORD, "index", "index_", templateUrl, "2", "23", indexer, null, null);
		pagination.add(selector);
		template.setPagination(pagination);

		// html
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("body > div.main > div.main_left.p5 > div > div.cont_b > div.cont_c", Constants.ATTRIBUTE_HTML);
		selector.initFieldSelector("page_content", "", indexer, null, null);
		news.add(selector);
		
		// title
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("body > div.main > div.main_left.p5 > div > div.cont_b > div.title", Constants.ATTRIBUTE_TEXT);
		selector.initFieldSelector("title", "", indexer, null, null);
		news.add(selector);

		// content
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer("body > div.main > div.main_left.p5 > div > div.cont_b > div.cont_c", Constants.ATTRIBUTE_TEXT);
		selector.initFieldSelector("content", "", indexer, null, null);
		news.add(selector);

		// tstamp
		selector = new Selector();
		indexer = new SelectorIndexer();
		indexer.initJsoupIndexer("body > div.main > div.main_left.p5 > div > div.cont_b > div.ly", Constants.ATTRIBUTE_TEXT);
		filter = new SelectorFilter();
		filter.initMatchFilter("\\d{4}-\\d{2}-\\d{2}");
		format = new SelectorFormat();
		format.initDateFormat("yyyy-MM-dd");
		selector.initFieldSelector("tstamp", "", indexer, filter, format);
		news.add(selector);
		template.setNews(news);
		RedisUtils.setTemplateResult(template, templateGuid,dbindex);
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
