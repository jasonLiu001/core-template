package com.isoftstone.crawl.template.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.isoftstone.crawl.template.itf.IFilterHandler;
import com.isoftstone.crawl.template.utils.StringUtil;

/**
 * RemoveFilter类是过滤器的实现类,主要提供替换过滤器
 * 
 * @author Tony Wang
 * @see IFilterHandler
 * @see SelectorFilter
 * 
 */
public class RemoveFilter implements IFilterHandler {
	private static final Logger LOG = LoggerFactory.getLogger(RemoveFilter.class);
	private SelectorFilter filter = null;

	public RemoveFilter() {

	}

	public RemoveFilter(SelectorFilter filter) {
		this.filter = filter;
	}

	/**
	 * TFIlterHandler接口方法实现
	 * 
	 * @param str
	 *            需要过滤的字符串
	 * @return 过滤后的字符串，如果为与模板中的匹配项匹配，则返回str的值
	 */
	@Override
	public String filter(String str) {
		String value = this.filter.getValue();
		// value为匹配项，str为需要匹配的内容
		if (!str.isEmpty() && !value.isEmpty()) {
			str = StringUtil.normalizingSpace(str);
			// value支持正则，可以替换掉匹配的内容
			return StringUtil.trim(str.replaceAll(value, ""));
		} else {
			LOG.error("Remove filter defined error.");
			return null;
		}
	}
}