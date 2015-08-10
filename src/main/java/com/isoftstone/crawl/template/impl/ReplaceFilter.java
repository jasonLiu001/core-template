package com.isoftstone.crawl.template.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.isoftstone.crawl.template.itf.IFilterHandler;
import com.isoftstone.crawl.template.utils.StringUtil;

/**
 * ReplaceFilter类是过滤器的实现类,主要提供替换过滤器
 * 
 * @author Tony Wang
 * @see IFilterHandler
 * @see SelectorFilter
 * 
 */
public class ReplaceFilter implements IFilterHandler {
	private static final Logger LOG = LoggerFactory.getLogger(RemoveFilter.class);
	private SelectorFilter filter = null;

	public ReplaceFilter() {

	}

	public ReplaceFilter(SelectorFilter filter) {
		this.filter = filter;
	}

	@Override
	public String filter(String str) {
		String value = this.filter.getValue();
		String replaceTo = this.filter.getReplaceTo();
		// value为匹配项，str为需要匹配的内容
		if (!str.isEmpty() && !value.isEmpty()) {
			str = StringUtil.normalizingSpace(str);
			// value支持正则，可以替换掉匹配的内容
			return StringUtil.trim(str.replaceAll(value, replaceTo));
		} else {
			LOG.error("Replace filter defined error.");
			return null;
		}
	}
}