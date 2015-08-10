package com.isoftstone.crawl.template.impl;

import java.util.ArrayList;
import java.util.List;

import com.isoftstone.crawl.template.global.Constants;
import com.isoftstone.crawl.template.itf.ISelectorHandler;

public class Selector extends BaseSelector {
	private String name = "";

	private String pagitationType = "";
	private String current = "";
	private String replaceTo = "";
	private String pagitationUrl = "";
	private String startNumber = "";
	private String lastNumber = "";
	private String recordNumber = "";
	private int interval = 0;

	private List<SelectorIndexer> indexers;
	private List<SelectorFilter> filters;
	private List<SelectorFormat> formats;
	private List<Selector> labels;
	private ISelectorHandler selector = null;

	/**
	 * 获取拾取器的名称，如article_title
	 * 
	 * @return 拾取器的名称
	 */
	public String getName() {
		return name;
	}

	/**
	 * 获取拾取器的名称，如article_title
	 * 
	 * @param name
	 *            拾取器的名称
	 */
	public void setName(String name) {
		this.name = name;
	}

	public String getPagitationType() {
		return pagitationType;
	}

	public void setPagitationType(String pagitationType) {
		this.pagitationType = pagitationType;
	}

	public String getCurrent() {
		return current;
	}

	public void setCurrent(String current) {
		this.current = current;
	}

	public String getReplaceTo() {
		return replaceTo;
	}

	public void setReplaceTo(String replaceTo) {
		this.replaceTo = replaceTo;
	}

	public String getPagitationUrl() {
		return pagitationUrl;
	}

	public void setPagitationUrl(String pagitationUrl) {
		this.pagitationUrl = pagitationUrl;
	}

	public String getStartNumber() {
		return startNumber;
	}

	public void setStartNumber(String startNumber) {
		this.startNumber = startNumber;
	}

	public String getLastNumber() {
		return lastNumber;
	}

	public void setLastNumber(String lastNumber) {
		this.lastNumber = lastNumber;
	}

	public String getRecordNumber() {
		return recordNumber;
	}

	public void setRecordNumber(String recordNumber) {
		this.recordNumber = recordNumber;
	}

	public int getInterval() {
		return interval;
	}

	public void setInterval(int interval) {
		this.interval = interval;
	}

	/**
	 * 获取类别属性，用于说明当前拾取器类型，如content|field|pagitation|label
	 * content，用于获取页面中的新闻页的URL列表 pagitation，用于获取页面中的分页的URL列表
	 * field，用于获取页面中的具体数据，如果标题、正文、发布时间等
	 * label，只用content关联使用，当列表页中包含具体的新闻页数据，如区域标签、行业标签等，通过label获取标签的值
	 * 
	 * @return 类别属性字符串
	 */
	@Override
	public String getType() {
		return super.getType();
	}

	/**
	 * 设置类别属性
	 * 
	 * @param type
	 *            类别属性字符串
	 */
	@Override
	public void setType(String type) {
		super.setType(type);
	}

	/**
	 * 获取拾取器的值，不通过索引器查询结果，直接返回值 TODO: 只用于label
	 * 
	 * @return 拾取器的值
	 */
	@Override
	public String getValue() {
		return super.getValue();
	}

	/**
	 * 设置拾取器的值，不通过索引器查询结果，直接返回值 TODO: 只用于label
	 * 
	 * @value 拾取器的值
	 */
	@Override
	public void setValue(String value) {
		super.setValue(value);
	}

	/**
	 * 获取拾取器中的索引器集合
	 * 
	 * @return 索引器集合
	 */
	public List<SelectorIndexer> getIndexers() {
		return indexers;
	}

	/**
	 * 设置拾取器中的索引器集合
	 * 
	 * @param indexers
	 *            索引器集合
	 */
	public void setIndexers(List<SelectorIndexer> indexers) {
		this.indexers = indexers;
	}

	/**
	 * 增加一个索引器到索引器集合
	 * 
	 * @param indexer
	 *            索引器
	 */
	public void setIndexer(SelectorIndexer indexer) {
		if (indexer == null)
			return;
		if (this.indexers == null)
			this.indexers = new ArrayList<SelectorIndexer>();
		this.indexers.add(indexer);
	}

	/**
	 * 获取拾取器中的过滤器集合
	 * 
	 * @return 过滤器集合
	 */
	public List<SelectorFilter> getFilters() {
		return filters;
	}

	/**
	 * 设置拾取器中的过滤器集合
	 * 
	 * @param filters
	 *            过滤器集合
	 */
	public void setFilters(List<SelectorFilter> filters) {
		this.filters = filters;
	}

	/**
	 * 增加一个过滤器到当前的过滤器集合
	 * 
	 * @param filter
	 *            过滤器
	 */
	public void setFilter(SelectorFilter filter) {
		if (filter == null)
			return;
		if (this.filters == null)
			this.filters = new ArrayList<SelectorFilter>();
		this.filters.add(filter);
	}

	/**
	 * 获取拾取器中的格式化器集合
	 * 
	 * @return 格式化器集合
	 */
	public List<SelectorFormat> getFormats() {
		return formats;
	}

	/**
	 * 设置拾取器中的格式化器集合
	 * 
	 * @param format
	 *            格式化器集合
	 */
	public void setFormats(List<SelectorFormat> formats) {
		this.formats = formats;
	}

	/**
	 * 增加一个格式化器到格式化器集合
	 * 
	 * @param format
	 */
	public void setFormat(SelectorFormat format) {
		if (format == null)
			return;
		if (this.formats == null)
			this.formats = new ArrayList<SelectorFormat>();
		this.formats.add(format);
	}

	/**
	 * 获取拾取器中的标签拾取器集合，只在Content中有效
	 * 
	 * @return 标签拾取器集合
	 */
	public List<Selector> getLabels() {
		return this.labels;
	}

	/**
	 * 设置拾取器中的标签拾取器集合，只在Content中有效
	 * 
	 * @param label
	 *            标签拾取器集合
	 */
	public void setLabels(List<Selector> labels) {
		this.labels = labels;
	}

	/**
	 * 增加一个标签拾取器到标签拾取器集合
	 * 
	 * @param label
	 *            标签拾取器
	 */
	public void setLabel(Selector label) {
		if (label == null) {
			return;
		}
		if (this.labels == null)
			this.labels = new ArrayList<Selector>();
		this.labels.add(label);
	}

	public Selector() {

	}

	public Selector(String type, String name, String value) {
		this.setType(type);
		this.setName(name);
		this.setValue(value);
	}

	/**
	 * process方法用于运行拾取器
	 * 
	 * @param input
	 *            需要查询的网页源代码的byte[]
	 * @param encoding
	 *            需要查询的网页的编码格式
	 * @param url
	 *            需要查询的网页的URL
	 * @param parseResult
	 *            当前页面对应的模板信息以及获取到的数据结果
	 * @return 查询状态
	 */
	public int process(byte[] input, String encoding, String url, ParseResult parseResult) {
		if (!this.getType().isEmpty()) {
			if (Constants.SELECTOR_FEILD.equals(this.getType())) {
				this.selector = new FieldSelector(this);
			} else if (Constants.SELECTOR_CONTENT.equals(this.getType())) {
				this.selector = new ContentSelector(this);
			} else if (Constants.SELECTOR_LABEL.equals(this.getType())) {
				this.selector = new LabelSelector(this);
			} else if (Constants.SELECTOR_PAGITATION.equals(this.getType())) {
				this.selector = new PaginationSelector(this);
			} else {
				return -1;
			}
		} else {
			return -1;
		}
		return this.selector.select(input, encoding, url, parseResult);
	}

	/**
	 * 初始化内容列表区域模板
	 * 
	 * @param indexers
	 *            索引器集合
	 * @param labels
	 *            标签拾取器集合
	 */
	public void initContentSelector(List<SelectorIndexer> indexers, List<Selector> labels) {
		this.setType(Constants.SELECTOR_CONTENT);
		this.setIndexers(indexers);
		this.setLabels(labels);
	}

	/**
	 * 初始化内容列表区域模板
	 * 
	 * @param indexer
	 *            索引器
	 * @param label
	 *            标签拾取器
	 */
	public void initContentSelector(SelectorIndexer indexer, Selector label) {
		this.setType(Constants.SELECTOR_CONTENT);
		this.setIndexer(indexer);
		this.setLabel(label);
	}

	/**
	 * 初始化分页列表区域模板
	 * 
	 * @param type
	 *            列表类别
	 * @param current
	 *            分页URL中，当前的字符串
	 * @param replaceTo
	 *            分页URL中，需要替换成的内容
	 * @param pagitationUrl
	 *            分页的URL
	 * @param start
	 *            分页的开始编号
	 * @param records
	 *            当前页面的新闻数量
	 * @param indexers
	 *            索引器集合
	 * @param filters
	 *            过滤器集合
	 */
	public void initPagitationSelector(String type, String current, String replaceTo, String pagitationUrl, String start, String records, List<SelectorIndexer> indexers, List<SelectorFilter> filters) {
		this.setType(Constants.SELECTOR_PAGITATION);
		this.setPagitationType(type);
		this.setCurrent(current);
		this.setReplaceTo(replaceTo);
		this.setPagitationUrl(pagitationUrl);
		this.setStartNumber(start);
		this.setRecordNumber(records);
		this.setIndexers(indexers);
	}

	/**
	 * 初始化分页列表区域模板
	 * 
	 * @param type
	 *            列表类别
	 * @param current
	 *            分页URL中，当前的字符串
	 * @param replaceTo
	 *            分页URL中，需要替换成的内容
	 * @param pagitationUrl
	 *            分页的URL
	 * @param start
	 *            分页的开始编号
	 * @param records
	 *            当前页面的新闻数量
	 * @param indexer
	 *            索引器
	 * @param filter
	 *            过滤器
	 */
	public void initPagitationSelector(String type, String current, String replaceTo, String pagitationUrl, String start, String records, SelectorIndexer indexer, SelectorFilter filter, SelectorFormat format) {
		this.setType(Constants.SELECTOR_PAGITATION);
		this.setPagitationType(type);
		this.setCurrent(current);
		this.setReplaceTo(replaceTo);
		this.setPagitationUrl(pagitationUrl);
		this.setStartNumber(start);
		this.setRecordNumber(records);
		this.setIndexer(indexer);
		this.setFilter(filter);
		this.setFormat(format);
	}

	public void initPagitationSelector(String type, String current, String replaceTo, String pagitationUrl, String start, String records, int interval, SelectorIndexer indexer, SelectorFilter filter, SelectorFormat format) {
		this.setType(Constants.SELECTOR_PAGITATION);
		this.setPagitationType(type);
		this.setCurrent(current);
		this.setReplaceTo(replaceTo);
		this.setPagitationUrl(pagitationUrl);
		this.setStartNumber(start);
		this.setRecordNumber(records);
		this.setInterval(interval);
		this.setIndexer(indexer);
		this.setFilter(filter);
		this.setFormat(format);
	}

	/**
	 * 自定义分页
	* @Title: initPagitationSelector 
	* @Description: TODO(这里用一句话描述这个方法的作用) 
	* @param type  列表类别
	* @param current  模板url中要替换的字符
	* @param pagitationUrl  模板url
	* @param start 分页开始页码
	* @param end 分页结束页码
	* @param interval  步进值 
	* @return void    返回类型 
	* @author lj
	* @throws
	 */
	public void initPagitationSelector(String type, String current,String replaceTo, String pagitationUrl, String start, String end, int interval) {
		this.setType(Constants.SELECTOR_PAGITATION);
		this.setPagitationType(type);
		this.setCurrent(current);
		this.setReplaceTo(replaceTo);
		this.setPagitationUrl(pagitationUrl);
		this.setStartNumber(start);
		this.setLastNumber(end);
		this.setInterval(interval);
	}

	/**
	 * 初始需要获取的字段模板
	 * 
	 * @param name
	 *            字段的名称
	 * @param value
	 *            字段的值，如果设置了字段值，则索引器、过滤器、格式化器无效
	 * @param indexers
	 *            索引器集合
	 * @param filters
	 *            过滤器集合
	 * @param formats
	 *            格式化器
	 */
	public void initFieldSelector(String name, String value, List<SelectorIndexer> indexers, List<SelectorFilter> filters, List<SelectorFormat> formats) {
		this.setType(Constants.SELECTOR_FEILD);
		this.setName(name);
		this.setValue(value);
		this.setIndexers(indexers);
		this.setFilters(filters);
		this.setFormats(formats);
	}

	/**
	 * 初始需要获取的字段模板
	 * 
	 * @param name
	 *            字段的名称
	 * @param value
	 *            字段的值，如果设置了字段值，则索引器、过滤器、格式化器无效
	 * @param indexers
	 *            索引器集合
	 * @param filters
	 *            过滤器集合
	 * @param formats
	 *            格式化器
	 */
	public void initFieldSelector(String name, String value, SelectorIndexer indexer, SelectorFilter filter, SelectorFormat format) {
		this.setType(Constants.SELECTOR_FEILD);
		this.setName(name);
		this.setValue(value);
		this.setIndexer(indexer);
		this.setFilter(filter);
		this.setFormat(format);
	}

	/**
	 * 初始需要获取的标签模板
	 * 
	 * @param name
	 *            标签的名称
	 * @param value
	 *            标签的值，如果设置了字段值，则索引器、过滤器、格式化器无效
	 * @param indexers
	 *            索引器集合
	 * @param filters
	 *            过滤器集合
	 * @param formats
	 *            格式化器集合
	 */
	public void initLabelSelector(String name, String value, List<SelectorIndexer> indexers, List<SelectorFilter> filters, List<SelectorFormat> formats) {
		this.setType(Constants.SELECTOR_LABEL);
		this.setName(name);
		this.setValue(value);
		this.setIndexers(indexers);
		this.setFilters(filters);
		this.setFormats(formats);
	}

	/**
	 * 初始需要获取的标签模板
	 * 
	 * @param name
	 *            标签的名称
	 * @param value
	 *            标签的值，如果设置了字段值，则索引器、过滤器、格式化器无效
	 * @param indexer
	 *            索引器
	 * @param filter
	 *            过滤器
	 * @param format
	 *            格式化器
	 */
	public void initLabelSelector(String name, String value, SelectorIndexer indexer, SelectorFilter filter, SelectorFormat format) {
		this.setType(Constants.SELECTOR_LABEL);
		this.setName(name);
		this.setValue(value);
		this.setIndexer(indexer);
		this.setFilter(filter);
		this.setFormat(format);
	}

	/**
	 * 重写toString()，用户输出Selector的模板数据
	 */
	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		str.append(super.toString());
		if (this.indexers != null) {
			str.append(this.indexers.toString());
		}
		if (this.filters != null) {
			for (SelectorFilter f : this.filters)
				str.append(f.toString());
		}
		if (this.formats != null) {
			for (SelectorFormat f : this.formats)
				str.append(f.toString());
		}
		if (this.labels != null) {
			for (Selector l : this.labels)
				str.append(l.toString());
		}
		return str.toString();
	}

}
