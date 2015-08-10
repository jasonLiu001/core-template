package com.isoftstone.crawl.template.impl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.isoftstone.crawl.template.global.Constants;
import com.isoftstone.crawl.template.itf.IIndexerHandler;
import com.isoftstone.crawl.template.utils.EncodeUtils;

/**
 * JoupIndexer类是索引器的实现类,主要提供通过JSOUP获取页面元素的功能
 *
 * @author Tony Wang
 * @see IIndexerHandler
 * @see SelectorIndexer
 */
public class JsoupIndexer implements IIndexerHandler {
    private static final Logger LOG = LoggerFactory.getLogger(JsoupIndexer.class);
    private SelectorIndexer index = null;

    public JsoupIndexer() {
    }

    public JsoupIndexer(SelectorIndexer index) {
        this.index = index;
    }

    /**
     * TIndexerHandler接口方法实现
     *
     * @param input    需要查询的网页源代码的byte[]
     * @param encoding 需要查询的网页的编码格式
     * @param url      需要查询的网页的URL
     * @return 获取的元素集会
     */
    @Override
    public ArrayList<String> index(byte[] input, String encoding, String url) {
        org.jsoup.nodes.Document doc = null;
        org.jsoup.select.Elements elements = null;
        InputStream in = new ByteArrayInputStream(input);
        try {
            String value = this.index.getValue();
            if (value != null && !value.isEmpty()) {
                value = EncodeUtils.convertEncoding(value, Constants.DEFAULT_ENCODING, encoding);
                doc = org.jsoup.Jsoup.parse(in, encoding, url);
                elements = doc.select(value);
            } else {
                LOG.error("JSOUP indexer defined error.");
                return null;
            }
            String attribute = this.index.getAttribute();
            if (attribute == null || attribute.isEmpty()) {
                this.index.setAttribute(Constants.ATTRIBUTE_TEXT);
            }
            return attributorHandler(elements, encoding);
        } catch (UnsupportedEncodingException e1) {
            LOG.error("JSOUP indexer defined error.");
            LOG.error(e1.getMessage());
        } catch (IOException e) {
            LOG.error("JSOUP indexer defined error.");
            LOG.error(e.getMessage());
        }
        return null;
    }


	/**
	 * attributorHandler方法，用于获取拾取元素的返回值
	 * 
	 * @param elements
	 *            查询到的元素集合
	 * @param encoding
	 *            需要查询的网页的编码格式
	 * @return 获取的元素集合
	 * @throws UnsupportedEncodingException 
	 */
	private ArrayList<String> attributorHandler(org.jsoup.select.Elements elements, String encoding) throws UnsupportedEncodingException {
		ArrayList<String> results = new ArrayList<String>();
		if (elements != null) {
			String r = null;
			for (org.jsoup.nodes.Element element : elements) {
				String attr = this.index.getAttribute();
				if (Constants.ATTRIBUTE_TEXT.equals(attr)) {
					r = element.text();
					//r = EncodeUtils.convertEncoding(r,encoding);
					results.add(r);
				} else if (Constants.ATTRIBUTE_HREF.equals(attr)) {
						results.add(EncodeUtils.formatUrl(element.absUrl("href"), ""));
				} else if (Constants.ATTRIBUTE_HTML.equals(attr)) {
					//r = element.html();
					r =element.outerHtml();
					//r = EncodeUtils.convertEncoding(r, encoding);
					results.add(r);
				} else if (Constants.ATTRIBUTE_SRC.equals(attr)) {
					results.add(element.absUrl("src"));
				} else {
					results.add(element.attr(attr));
				}
			}
		}
		return results;
	}
}