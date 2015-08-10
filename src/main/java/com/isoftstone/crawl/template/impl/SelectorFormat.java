package com.isoftstone.crawl.template.impl;


import com.isoftstone.crawl.template.global.Constants;
import com.isoftstone.crawl.template.itf.IFormaterHandler;

/**
 * SelectorFormat类定义了格式化器，用于根据当前字符串的格式，将当前字符串格式化为系统设定的默认格式
 * 
 * @example {"type":"match", "value":"yyyy年MM月dd日 HH:mm"}，用于将格式为“yyyy年MM月dd日
 *          HH:mm”的时间字符串，转换为系统设定的时间格式，当前默认时间格式为(yyyy-MM-dd HH:mm)
 * 
 * @author Tony Wang
 * 
 */
public class SelectorFormat extends BaseSelector {
	private IFormaterHandler formaterHandler = null;

	/**
	 * 获取类别属性，用于说明当前格式化器类型，如Date
	 * 
	 * @return 类别属性字符串
	 */
	@Override
	public String getType() {
		return super.getType();
	}

	/**
	 * 设置类别属性，用于说明当前格式化器类型，如Date
	 * 
	 * @param type
	 *            类别属性字符串
	 */
	@Override
	public void setType(String type) {
		super.setType(type);
	}

	/**
	 * 获取值属性，用于描述格式化器中当前文档的格式
	 * 
	 * @return 格式化器的值
	 */
	@Override
	public String getValue() {
		return super.getValue();
	}

	/**
	 * 设置值属性，用于描述格式化器中当前文档的格式
	 * 
	 * @param value
	 *            格式化器的值
	 */
	@Override
	public void setValue(String value) {
		super.setValue(value);
	}

	public SelectorFormat() {

	}

	public void initDateFormat(String value) {
		this.setType(Constants.FORMAT_DATE);
		this.setValue(value);
	}

	/**
	 * 返回SelectorFormat对象的字符串
	 * 
	 * @return SelectorFormat模板内容
	 */
	@Override
	public String toString() {
		return super.toString();
	}

	/**
	 * getHandler方法用于获取格式化器，根据格式化器的类别实例化具体的格式化器
	 */
	private void getHandler() {
		String type = getType();
		String value = getValue();
		if (type != null && !type.isEmpty() && value != null
				&& !value.isEmpty()) {
			if (Constants.FORMAT_DATE.equals(type)) {
				this.formaterHandler = new DateFormat(this);
			}
		} else {
			// TODO: type is null or value is null
		}
	}

	/**
	 * process方法用于运行格式化器
	 * 
	 * @param str
	 *            需要被格式化的字符串
	 * @return 根据当前字符串的格式，将当前字符串格式化为系统设定的默认格式
	 */
	public String process(String str) {
		if (this.formaterHandler == null)
			getHandler();
		return this.formaterHandler.format(str);
	}
}
