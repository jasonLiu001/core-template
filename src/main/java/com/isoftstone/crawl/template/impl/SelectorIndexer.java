package com.isoftstone.crawl.template.impl;

import java.util.ArrayList;

import com.isoftstone.crawl.template.global.Constants;
import com.isoftstone.crawl.template.itf.IIndexerHandler;

/**
 * SelectorFilter类定义了索引器，用于根据索引器模板，获取到需要的数据。
 * 
 * @example {"type":"jsoup",
 *          "value":"div.classname table tbody tr td a"}，用于获取页面上的特定元素
 * 
 * @author Tony Wang
 * 
 */
public class SelectorIndexer extends BaseSelector {
	private String attribute = "";
	private IIndexerHandler idexerHandler = null;

	/**
	 * 获取类别属性，用于说明当前索引器类型，如jsoup
	 * 
	 * @return 类别属性字符串
	 */
	@Override
	public String getType() {
		return super.getType();
	}

	/**
	 * 设置类别属性，用于说明当前索引器类型
	 * 
	 * @param type
	 *            类别属性字符串
	 */
	@Override
	public void setType(String type) {
		super.setType(type);
	}

	/**
	 * 获取值属性，用于描述索引器的查询语句，如div.tableclass table tr td a
	 * 
	 * @return 索引器的值
	 */
	@Override
	public String getValue() {
		return super.getValue();
	}

	/**
	 * 设置值属性，用于描述索引器的查询语句
	 * 
	 * @param value
	 *            索引器的值
	 */
	@Override
	public void setValue(String value) {
		super.setValue(value);
	}

	/**
	 * 获取属性值，用于描述索引器的查询结果需要获取的属性，如a标签的href属性，如img标签的src，如整个标签的text
	 * 
	 * @return 属性值
	 */
	public String getAttribute() {
		return attribute;
	}

	/**
	 * 获取属性值，用于描述索引器的查询结果需要获取的属性，如a标签的href属性，如img标签的src，如整个标签的text
	 * 
	 * @param attribute
	 *            属性值
	 */
	public void setAttribute(String attribute) {
		this.attribute = attribute;
	}

	public SelectorIndexer() {

	}

	public void initJsoupIndexer(String value, String attr) {
		this.setType(Constants.INDEXER_JOUP);
		this.setValue(value);
		this.setAttribute(attr);
	}

	/**
	 * getHandler方法用于获取索引器，根据索引器的类别以及查语句，获取页面上的指定元素
	 */
	private void getHandler() {
		if (getType() != null && !getType().isEmpty() && getValue() != null
				&& !getValue().isEmpty()) {
			if (Constants.INDEXER_JOUP.equals(getType())) {
				this.idexerHandler = new JsoupIndexer(this);
			}
		} else {
			// TODO: type is null or value is null
		}
	}

	/**
	 * process方法用于执行索引器，根据索引器的类别以及查询语句，获取页面上的指定元素
	 * 
	 * @param input
	 *            需要查询的页面源代码
	 * @param encoding
	 *            需要查询的页面的字符编码规则，如utf8、GB2312
	 * @param url
	 *            当前需要查询的页面地址
	 * 
	 * @return 根据索引器的查询语句获取的指定数据的列表
	 */
	public ArrayList<String> process(byte[] input, String encoding, String url) {
		if (this.idexerHandler == null)
			getHandler();
		return this.idexerHandler.index(input, encoding, url);
	}

	/**
	 * 重写toString()，用户输出SelectorIndexer的模板数据
	 */
	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		str.append(super.toString());
		if (this.attribute != null) {
			str.append("ATTRIBUTE: " + this.attribute + "\n");
		}
		return str.toString();
	}
}