package com.isoftstone.crawl.template.impl;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.isoftstone.crawl.template.itf.IFormaterHandler;

/**
 * DateFormater类是格式化器的实现类,主要提供时间字符串的格式
 * 
 * @author Tony Wang
 * @see IFormaterHandler
 * @see SelectorFormat
 * 
 */
public class DateFormat implements IFormaterHandler {
	private static final Logger LOG =LoggerFactory.getLogger(DateFormat.class);
	private static final SimpleDateFormat DEFAULT_DATEFORMAT = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm");
	private SelectorFormat format = null;

	public DateFormat() {

	}

	public DateFormat(SelectorFormat format) {
		this.format = format;
	}

	/**
	 * TFormaterHandler接口方法实现
	 * 
	 * @param str
	 *            需要格式化的时间字符串
	 * @return 格式化成默认的时间格式的时间字符串
	 */
	@Override
	public String format(String str) {
		String dateStr = str;
		Date date = null;
		String value = this.format.getValue();
		try {
			// value为匹配项，str为需要匹配的内容
			if (value != null && !value.isEmpty() && str != null
					&& !str.isEmpty()) {
				SimpleDateFormat curFormat = new SimpleDateFormat(value,
						Locale.CHINA);
				date = curFormat.parse(str);
				dateStr = DEFAULT_DATEFORMAT.format(date);
			} else {
				LOG.error("Date Format defined error.");
			}
		} catch (ParseException e) {
			LOG.error("Date Format value defined error.");
			LOG.error(e.getMessage());
		}
		return dateStr;
	}
}