package com.isoftstone.crawl.template.impl;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.isoftstone.crawl.template.global.Constants;
import com.isoftstone.crawl.template.itf.ISelectorHandler;

public class ContentSelector implements ISelectorHandler {
	private static final Logger LOG =LoggerFactory.getLogger(ContentSelector.class);
	private Selector selector = null;

	public ContentSelector() {

	}

	public ContentSelector(Selector selector) {
		this.selector = selector;
	}

	@Override
	public int select(byte[] input, String encoding, String url, ParseResult parseResult) {
		List<SelectorIndexer> indexers = selector.getIndexers();
		List<Selector> labels = selector.getLabels();

		// 当index ！= null时，通过索引器获取数据 
		if (indexers != null) {
			int outlinkLength = 0;
			int count = 0;
			for (int i = 0; i < indexers.size(); i++) {
				ArrayList<String> results = indexers.get(i).process(input, encoding, url);
				int number = results.size();
				if (number > 0) {
					for (int j = 0; j < number; j++) {
						// if(count >= 5)//一个页面中重复链接大于5，则退出
						// {
						// count = number;
						// break;
						// }
						String resultVale = results.get(j);
						// 判断当前链接是否已经存在
//						boolean exists = RedisUtils.contains(MD5Utils.MD5(resultVale));
//						if (!exists) {
							parseResult.setResult(Constants.CONTENT_OUTLINK + "_" + j, resultVale);
//						} else {
//							LOG.info("Already exists url " + resultVale);
//							count++;
//						}
					}
					outlinkLength = number;
				}
			}
			parseResult.setResult(Constants.CONTENT_OUTLINK, String.valueOf(outlinkLength - count));
			// 处理标签拾取器
			StringBuilder str = new StringBuilder();
			if (labels != null && labels.size() > 0) {
				for (Selector label : labels) {
					int flag = label.process(input, encoding, url, parseResult);
					if (flag == 1) {
						str.append(label.getName() + ",");
					}
				}
				parseResult.setResult(Constants.LABEL_LIST, str.toString());
			}
			return 1;
		} else {
			LOG.error("The page " + url + " , the selector CONTENT_OUTLINK the index is null.");
			return -1;
		}
	}
}
