package com.isoftstone.crawl.template.impl;


import com.isoftstone.crawl.template.global.Constants;
import com.isoftstone.crawl.template.itf.IFilterHandler;

/**
 * SelectorFilter类定义了过滤器，用于将拾取器(Selector)中获取到的数据，进行过滤处理。
 * 
 * @example {"type":"remove", "value":"the string removed"}，用于替换无用的字符串
 * @example {"type":"match", "value":"regular expression"}，用于获取匹配的字符串
 * @author Tony Wang
 * 
 */
public class SelectorFilter extends BaseSelector {
	private IFilterHandler filterHandler = null;

	private String replaceTo = "";

	public String getReplaceTo() {
		return replaceTo;
	}

	public void setReplaceTo(String replaceTo) {
		this.replaceTo = replaceTo;
	}

	/**
	 * 获取类别属性，用于说明当前过滤器类型，如Remove或Match
	 * 
	 * @return 类别属性字符串
	 */
	@Override
	public String getType() {
		return super.getType();
	}

	/**
	 * 设置类别属性，用于说明当前过滤器类型，如Remove或Match
	 * 
	 * @param type
	 *            类别属性字符串
	 */
	@Override
	public void setType(String type) {
		super.setType(type);
	}

	/**
	 * 获取值属性，用于描述过滤器中匹配项的值
	 * 
	 * @return 匹配项的值
	 */
	@Override
	public String getValue() {
		return super.getValue();
	}

	/**
	 * 设置值属性，用于描述过滤器中匹配项的值
	 * 
	 * @param value
	 *            匹配项的值
	 */
	@Override
	public void setValue(String value) {
		super.setValue(value);
	}

	public SelectorFilter() {

	}

	public void initReplaceFilter(String value, String replaceTo) {
		this.setType(Constants.FILTER_REPLACE);
		this.setReplaceTo(replaceTo);
		this.setValue(value);
	}

	public void initRemoveFilter(String value) {
		this.setType(Constants.FILTER_REMOVE);
		this.setValue(value);
	}

	public void initMatchFilter(String value) {
		this.setType(Constants.FILTER_MATCH);
		this.setValue(value);
	}

	/**
	 * 返回SelectorFilter对象的字符串
	 * 
	 * @return SelectorFilter模板内容
	 */
	@Override
	public String toString() {
		return super.toString();
	}

	/**
	 * getHandler方法用于获取过滤器，根据过滤器的类别实例化具体的过滤器
	 */
	private void getHandler() {
		String type = this.getType();
		String value = this.getValue();
		if (type != null && !type.isEmpty() && value != null
				&& !value.isEmpty()) {

			if (Constants.FILTER_REMOVE.equals(type)) {
				this.filterHandler = new RemoveFilter(this);
			} else if (Constants.FILTER_MATCH.equals(type)) {
				this.filterHandler = new MatchFilter(this);
			} else if (Constants.FILTER_REPLACE.equals(type)) {
				this.filterHandler = new ReplaceFilter(this);
			}
		} else {
			// TODO: type is null or value is null
		}
	}

	/**
	 * process方法用于运行过滤器
	 * 
	 * @param str
	 *            需要被过滤的字符串
	 * @return 过滤后的字符串，当过滤器未找到需要处理的内容时，返回原字符串；当过滤器定义不正确时，返回原字符串
	 */
	public String process(String str) {
		if (this.filterHandler == null)
			this.getHandler();
		return this.filterHandler.filter(str);
	}
}
