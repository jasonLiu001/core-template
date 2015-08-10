package com.isoftstone.crawl.template.impl;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.isoftstone.crawl.template.itf.IFilterHandler;
import com.isoftstone.crawl.template.utils.StringUtil;

/**
 * MatchFilter类是过滤器的实现类,主要提供正则表达式过滤器
 * 
 * @author Tony Wang
 * @see IFilterHandler
 * @see SelectorFilter
 * 
 */
public class MatchFilter implements IFilterHandler {
	private static final Logger LOG = LoggerFactory.getLogger(MatchFilter.class);
	private SelectorFilter filter = null;

	public MatchFilter() {

	}

	public MatchFilter(SelectorFilter filter) {
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
		String value = filter.getValue();
		String result ="";
		// value为匹配项，str为需要匹配的内容
		if (str != null && !str.isEmpty() && value != null && !value.isEmpty()) {
			str = StringUtil.normalizingSpace(str);
			Pattern p = Pattern.compile(value, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
			Matcher m = p.matcher(str);
			if (m.find()) {
				result=StringUtil.trim(m.group(1));
			}
		} else {
			// str或value为空，返回null，并输出error
			LOG.error("Match filter defined error.");
			return null;
		}
		// 如果未找到需要匹配的内容，则返回""值
		if (result.length() == 0) {
			LOG.warn("Don't get the match data (" + value + ") from " + str + ".");
		}
		return result;
	}
}