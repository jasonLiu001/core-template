package com.isoftstone.crawl.template.impl;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.isoftstone.crawl.template.itf.ISelectorHandler;

/**
 * FieldSelector类是拾取器的实现类,主要提供正针对新闻页中的各种信息的检索爬取
 *
 * @author Tony Wang
 * @see ISelectorHandler
 * @see Selector
 */
public class FieldSelector implements ISelectorHandler {

    private static final Logger LOG = LoggerFactory.getLogger(FieldSelector.class);
    private Selector selector = null;

    public FieldSelector() {

    }

    public FieldSelector(Selector selector) {
        this.selector = selector;
    }

    /**
     * TSelectorHandler接口方法实现
     *
     * @param input       需要查询的网页源代码的byte[]
     * @param encoding    需要查询的网页的编码格式
     * @param url         需要查询的网页的URL
     * @param parseResult 当前页面对应的模板信息以及获取到的数据结果
     * @return 查询状态
     */
    @Override
    public int select(byte[] input, String encoding, String url, ParseResult parseResult) {
        List<SelectorIndexer> indexers = this.selector.getIndexers();
        String resultKey = this.selector.getName();
        // parseResult中保存了当前新闻页的URL以及标签信息
        if (parseResult == null) {
            LOG.error("This page " + url + " ， don't init the parse result object in redis.");
            return -1;
        } // field拾取器必须定义name属性，无name属性，获取到的结果无法保存
        if (resultKey.isEmpty()) {
            LOG.error("This page " + url + " ， the name of this selector is null or empty.");
            return -1;
        }
        // TODO: 已经获取的同名Selector不再获取
        /*
		 * if (parseResult.getResult() == null) { LOG.debug("The selector of " +
		 * resultKey + " has got."); return -1; }
		 */
        // 当index ！= null时，通过索引器获取数据
        if (indexers != null) {
            StringBuilder field = new StringBuilder();
            for (int i = 0; i < indexers.size(); i++) {
                ArrayList<String> results = indexers.get(i).process(input, encoding, url);
                if (results.size() == 1) {
                    String resultValue = results.get(0);
                    if (resultValue.isEmpty()) {
                        LOG.error("The page " + url + " , the selector " + resultKey + " , the index " + " , get the result is empty");
                        return -1;
                    }
                    // 处理过滤器
                    List<SelectorFilter> filters = selector.getFilters();
                    if (filters != null) {
                        for (int j = 0; j < filters.size(); j++) {
                            resultValue = filters.get(j).process(resultValue);
                        }
                    }
                    // 处理格式化器
                    List<SelectorFormat> formats = selector.getFormats();
                    if (formats != null && (formats.size() > 0)) {
                        for (int j = 0; j < formats.size(); j++) {
                            resultValue = formats.get(j).process(resultValue);
                        }
                    }
                    field.append(resultValue);
                }
            }
            parseResult.setResult(resultKey, field.toString());
            return 1;
        } else {

            String resultValue = selector.getValue();
            if (!resultValue.isEmpty()) {
                parseResult.setResult(resultKey, resultValue);
                return 1;
            } else {
                LOG.error("The page " + url + " , the selector " + resultKey + " , both of the index and value are empty!");
                return -1;
            }
        }
    }
}
