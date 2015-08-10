package com.isoftstone.crawl.template.impl;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.isoftstone.crawl.template.global.Constants;
import com.isoftstone.crawl.template.itf.ISelectorHandler;

public class LabelSelector implements ISelectorHandler {
	private static final Logger LOG =LoggerFactory.getLogger(LabelSelector.class);
	private Selector selector;

	public LabelSelector() {

	}

	public LabelSelector(Selector selector) {
		this.selector = selector;
	}

	@Override
	public int select(byte[] input, String encoding, String url, ParseResult parseResult) {
		List<SelectorIndexer> indexs = this.selector.getIndexers();
		String resultKey = this.selector.getName();
		// label拾取器必须定义name属性，无name属性，获取到的结果无法保存
		if (resultKey.isEmpty()) {
			LOG.error("Label selector defined error without name.");
			return -1;
		}
		ArrayList<String> results = null;
		if (indexs != null && indexs.size() == 1) {
			SelectorIndexer index = indexs.get(0);
			results = index.process(input, encoding, url);
			if (results == null) {
				LOG.error("Don't get the result from label selector or results size isn't equal with the number of outlinks.");
				return 0;
			} else {
//				if (!parseResult.getResult().containsKey(resultKey)) {
//					parseResult.setResult(Constants.LABEL_LIST, resultKey);
//				} else {
//					String temp = parseResult.getResult().containsKey(resultKey) + "," + resultKey;
//					parseResult.setResult(Constants.LABEL_LIST, temp);
//				}				
				parseResult
						.setResult(resultKey, String.valueOf(results.size()));
				for (int i = 0; i < results.size(); i++) {
					List<SelectorFilter> filters = selector.getFilters();
					String resultValue = results.get(i);
					if (filters != null && (filters.size() > 0)) {
						for (SelectorFilter filter : filters) {
							resultValue = filter.process(resultValue);
						}
					}
					List<SelectorFormat> formats = selector.getFormats();
					if (formats != null && (formats.size() > 0)) {
						for (SelectorFormat format : formats) {
							resultValue = format.process(resultValue);
						}
					}
					parseResult.setResult(resultKey + "_" + i, resultValue);
				}
			}
			return 1;
		} else {
			String resultValue = selector.getValue();
			if (resultValue != null && !resultValue.isEmpty()) {
				int contentLength = Integer.parseInt(parseResult.getResult()
						.get(Constants.CONTENT_OUTLINK));
				parseResult.setResult(resultKey, String.valueOf(contentLength));
				for (int i = 0; i < contentLength; i++) {
					parseResult.setResult(resultKey + "_" + i, resultValue);
				}
				return 1;
			} else {
				LOG.error("Label selector defined error");
				return -1;
			}
		}
	}
}
