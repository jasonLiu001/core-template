package com.isoftstone.crawl.template.test;

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
import com.isoftstone.crawl.template.utils.DownloadHtml;
import com.isoftstone.crawl.template.utils.MD5Utils;
import com.isoftstone.crawl.template.utils.RedisUtils;

public class FuJianGovTest {
	public static void main(String[] args) {
		
		//生成解析模板
		String url = "http://www.ccgp-fujian.gov.cn/secpag.cfm?caidan=%B2%C9%B9%BA%B9%AB%B8%E6&caidan2=%B9%AB%BF%AA%D5%D0%B1%EA&level=province&area=&yqgg=0";
		String encoding = "gb2312";
		byte[] input = DownloadHtml.getHtml(url);
		TemplateResult templateResult = FuJianGovTemplate();
		System.out.println("templateResult:" + templateResult.toJSON());
		
		//解析列表页
		ParseResult parseResult = null;
		parseResult = TemplateFactory.process(input, encoding, url,Constants.DEFAULT_REDIS_DBINDEX);		
		System.out.println(parseResult.toJSON());
		
		//解析内容页
		url = "http://www.ccgp-fujian.gov.cn/Article.cfm?id=346870&caidan=%B2%C9%B9%BA%B9%AB%B8%E6&caidan2=%B9%AB%BF%AA%D5%D0%B1%EA&level=province&yqgg=0";
		input = DownloadHtml.getHtml(url);
		encoding = "gb2312";
		parseResult = TemplateFactory.process(input, encoding, url,Constants.DEFAULT_REDIS_DBINDEX);
		System.out.println(parseResult.toJSON());
	}

	public static TemplateResult FuJianGovTemplate() {
		TemplateResult template = new TemplateResult();
		template.setType(Constants.TEMPLATE_LIST);
		String templateUrl = "http://www.ccgp-fujian.gov.cn/secpag.cfm?caidan=%B2%C9%B9%BA%B9%AB%B8%E6&caidan2=%B9%AB%BF%AA%D5%D0%B1%EA&level=province&area=&yqgg=0";
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

      // 构建列表模板 列表项
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer(
				"body > table> tbody> tr:nth-child(3) > td> table> tbody> tr> td:nth-child(2) > table> tbody> tr> td > table > tbody > tr:nth-child(2) > td > table > tbody > tr > td > table > tbody > tr:nth-child(2) > td > table > tbody > tr:nth-child(2) > td > table > tbody>tr>td:nth-child(1)>a",
				Constants.ATTRIBUTE_HREF);
		selector.initContentSelector(indexer, null);

		//列表中 各个列表项时间
		Selector label = new Selector();
		label.setType(Constants.SELECTOR_LABEL);
		indexer = new SelectorIndexer();
		indexer.initJsoupIndexer(
				"body > table> tbody> tr:nth-child(3) > td> table> tbody> tr> td:nth-child(2) > table> tbody> tr> td > table > tbody > tr:nth-child(2) > td > table > tbody > tr > td > table > tbody > tr:nth-child(2) > td > table > tbody > tr:nth-child(2) > td > table > tbody>tr>td:nth-child(2)",
				Constants.ATTRIBUTE_TEXT);
		//选取过滤
		filter = new SelectorFilter();
		filter.initMatchFilter(Constants.YYYYMMDD);
		label.initLabelSelector("tstamp", "", indexer, filter, null);
		selector.setLabel(label);
		list.add(selector);
		template.setList(list);

		// 分页
		indexer = new SelectorIndexer();
		selector = new Selector();
		//选取的对象总页数
		indexer.initJsoupIndexer(
				"body > table > tbody > tr:nth-child(3) > td > table > tbody > tr > td:nth-child(2) > table > tbody > tr > td > table > tbody > tr:nth-child(2) > td > table > tbody > tr > td > table > tbody > tr:nth-child(2) > td > table > tbody > tr:nth-child(2) > td > table > tbody > tr:nth-child(27) > td > font > a:nth-child(2)",
				Constants.ATTRIBUTE_HREF);
		filter = new SelectorFilter();
		//获取总页数
		filter.initMatchFilter("\\?PageNum=(\\d+)");
		//构建分页的选取链接
		selector.initPagitationSelector(
				Constants.PAGINATION_TYPE_PAGENUMBER,
				"##",
				"",
				"http://www.ccgp-fujian.gov.cn/secpag.cfm?PageNum=##&&caidan=采购公告&caidan2=公开招标&level=province&yqgg=0",				
				"2", null, indexer, filter, null);
		pagination.add(selector);
		template.setPagination(pagination);

		// title
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer(
				"body > table > tbody > tr:nth-child(3) > td > table > tbody > tr:nth-child(2) > td > table > tbody > tr > td > font",
				Constants.ATTRIBUTE_TEXT);
		selector.initFieldSelector("title", "", indexer, null, null);
		news.add(selector);

		// content
		indexer = new SelectorIndexer();
		selector = new Selector();
		indexer.initJsoupIndexer(
				"body > table > tbody > tr:nth-child(3) > td > table > tbody > tr:nth-child(2) > td > table > tbody > tr:nth-child(3) > td",
				Constants.ATTRIBUTE_TEXT);
		selector.initFieldSelector("content", "", indexer, null, null);
		news.add(selector);
		template.setNews(news);

		RedisUtils.setTemplateResult(template, templateGuid,0);
		return template;
	}

}
