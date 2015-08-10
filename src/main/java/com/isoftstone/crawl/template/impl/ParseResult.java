package com.isoftstone.crawl.template.impl;

import java.util.HashMap;
import java.util.Map;

import com.isoftstone.crawl.template.utils.JSONUtils;

/**
 * ParseResult类用于存储通过模板解析到的数据
 * 
 * @author Tony Wang
 * 
 */
public class ParseResult {
	private String templateGuid;
	private Map<String, String> result;
	/**
	 * 获取模板的GUID
	 * 
	 * @return 模板的GUID
	 */
	public String getTemplateGuid() {
		return templateGuid;
	}

	/**
	 * 设置模板的GUID
	 * 
	 * @param templateGuid
	 *            模板的GUID
	 */
	public void setTemplateGuid(String templateGuid) {
		this.templateGuid = templateGuid;
	}

	/**
	 * 获取页面结果
	 * 
	 * @return 页面结果键值对
	 */
	public Map<String, String> getResult() {
		return result;
	}

	/**
	 * 设置页面的结果
	 * 
	 * @param result
	 *            页面结果键值对
	 */
	public void setResult(Map<String, String> result) {
		this.result = result;
	}

	/**
	 * 设置页面的结果
	 * 
	 * @param key
	 *            页面结果的名称
	 * @param value
	 *            页面结果的值
	 */
	public void setResult(String key, String value) {
		if (this.result == null) {
			this.result = new HashMap<String, String>();
		}
		this.result.put(key, value);
	}

	
	/**
	 * 重写toString()，用户输出ParseResult所有属性数据
	 */
	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		str.append(super.toString() + "\n");
		if (result != null) {
			for (String key : result.keySet()) {
				str.append(key + " : " + result.get(key) + "\n");
			}
		} else {
			str.append(super.toString());
		}
		return str.toString();
	}

	public String toJSON() {
		return JSONUtils.getParseResultJSON(this);
	}
}
