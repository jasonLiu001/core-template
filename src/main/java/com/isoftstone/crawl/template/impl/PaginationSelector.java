package com.isoftstone.crawl.template.impl;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.isoftstone.crawl.template.global.Constants;
import com.isoftstone.crawl.template.itf.ISelectorHandler;

public class PaginationSelector implements ISelectorHandler {
	private static final Logger LOG = LoggerFactory.getLogger(PaginationSelector.class);
	private Selector selector;

	public PaginationSelector() {

	}

	public PaginationSelector(Selector selector) {
		this.selector = selector;
	}

	@Override
	public int select(byte[] input, String encoding, String url, ParseResult parseResult) {
		String type = this.selector.getPagitationType();
		List<SelectorIndexer> indexers = selector.getIndexers();
		String current = this.selector.getCurrent();
		String replaceTo = this.selector.getReplaceTo();
		String pagitationUrl = this.selector.getPagitationUrl();
		String startNumber = this.selector.getStartNumber();
		String endNumber = this.selector.getLastNumber();
		int interval = this.selector.getInterval();
		
		if (Constants.PAGINATION_TYPE_PAGE.equals(type)) {
			if (indexers != null) {
				ArrayList<String> outlinks = new ArrayList<String>();
				for (int i = 0; i < indexers.size(); i++) {
					ArrayList<String> results = indexers.get(i).process(input, encoding, url);
					if (results.size() > 0) {
						outlinks.addAll(results);
					}
				}
				parseResult.setResult(Constants.PAGINATION_OUTLINK, String.valueOf(outlinks.size()));
				for (int i = 0; i < outlinks.size(); i++) {
					parseResult.setResult(Constants.PAGINATION_OUTLINK + "_" + i, outlinks.get(i));
				}
				return 1;
			} else {
				LOG.error("Pagitation selector defined error.");
				return -1;
			}
		} else if (Constants.PAGINATION_TYPE_PAGENUMBER.equals(type)) {
			if (indexers != null) {
				String lastNumber = "0";
				for (int i = 0; i < indexers.size(); i++) {
					ArrayList<String> results = indexers.get(i).process(input, encoding, url);
					if (results.size() > 0) {
						lastNumber = results.get(0);
						break;
					}
				}
				// 处理过滤器
				List<SelectorFilter> filters = selector.getFilters();
				if (filters != null) {
					for (int j = 0; j < filters.size(); j++) {
						lastNumber = filters.get(j).process(lastNumber);
					}
				}
				int last = Integer.parseInt(lastNumber);
				int start = Integer.parseInt(startNumber);
				// 默认返回最多页数
				int pageCount = last > Constants.MAX_PAGE_COUNT ? Constants.MAX_PAGE_COUNT : last;
				parseResult.setResult(Constants.PAGINATION_OUTLINK, String.valueOf(pageCount));
				for (int i = 0; i < pageCount; i++) {
					parseResult.setResult(Constants.PAGINATION_OUTLINK + "_" + i, pagitationUrl.replace(current, replaceTo + start));
					start++;
				}
				return 1;
			} else {
				LOG.error("Pagitation selector defined error.");
				return -1;
			}
		} else if (Constants.PAGINATION_TYPE_PAGERECORD.equals(type)) {
			if (indexers != null) {
				String totalRecordNumber = "0";
				for (int i = 0; i < indexers.size(); i++) {
					ArrayList<String> results = indexers.get(i).process(input, encoding, url);
					if (results.size() > 0) {
						totalRecordNumber = results.get(0);
						break;
					}
				}
				// 处理过滤器
				List<SelectorFilter> filters = selector.getFilters();
				if (filters != null) {
					for (int j = 0; j < filters.size(); j++) {
						totalRecordNumber = filters.get(j).process(totalRecordNumber);
					}
				}
				double total = Double.parseDouble(totalRecordNumber);
				double record = Double.parseDouble(this.selector.getRecordNumber());
				int last = (int) Math.ceil(total / record);
				int start = Integer.parseInt(startNumber);
				// 默认返回最多页数
				int pageCount = last > Constants.MAX_PAGE_COUNT ? Constants.MAX_PAGE_COUNT : last;

				parseResult.setResult(Constants.PAGINATION_OUTLINK, String.valueOf(last - 1));
				for (int i = 0; i < pageCount; i++) {
					parseResult.setResult(Constants.PAGINATION_OUTLINK + "_" + i, pagitationUrl.replace(current, replaceTo + start));
					start++;
				}
				return 1;
			} else {
				LOG.error("Pagination selector defined error.");
				return -1;
			}
		} else if (Constants.PAGINATION_TYPE_PAGENUMBER_INTERVAL.equals(type)) {
			if (indexers != null) {
				String lastNumber = "0";
				for (int i = 0; i < indexers.size(); i++) {
					ArrayList<String> results = indexers.get(i).process(input, encoding, url);
					if (results.size() > 0) {
						lastNumber = results.get(0);
						break;
					}
				}
				// 处理过滤器
				List<SelectorFilter> filters = selector.getFilters();
				if (filters != null) {
					for (int j = 0; j < filters.size(); j++) {
						lastNumber = filters.get(j).process(lastNumber);
					}
				}
				int last = Integer.parseInt(lastNumber);
				// 默认返回最多页数
				int pageCount = last > Constants.MAX_PAGE_COUNT ? Constants.MAX_PAGE_COUNT : last;
				parseResult.setResult(Constants.PAGINATION_OUTLINK, String.valueOf(pageCount));
				for (int i = 0; i < pageCount; i++) {
					parseResult.setResult(Constants.PAGINATION_OUTLINK + "_" + i, pagitationUrl.replace(current, String.valueOf(interval * (i+1))));
				}
				return 1;
			} else {
				LOG.error("Pagitation selector defined error.");
				return -1;
			}
		} else if (Constants.PAGINATION_TYPE_CUSTOM.equals(type)){
			int start = Integer.parseInt(startNumber);//开始页码数
			int pageNumber =Integer.parseInt(endNumber);//总页码数
			parseResult.setResult(Constants.PAGINATION_OUTLINK, endNumber);
			for (int i = start; i <= pageNumber; i++) {
				parseResult.setResult(Constants.PAGINATION_OUTLINK+ "_" + i,pagitationUrl.replace(current, replaceTo+String.valueOf(i*interval)));
			}
//			for (int i = 0; i <pageNumber; i++) {//支持翻页页码递减
//				int temp = start+(i*interval);
//				parseResult.setResult(Constants.PAGINATION_OUTLINK+ "_" + i,pagitationUrl.replace(current, replaceTo+String.valueOf(temp)));
//			}
			return 1;
		}else
		{
			return -1;
		}
	}
}
