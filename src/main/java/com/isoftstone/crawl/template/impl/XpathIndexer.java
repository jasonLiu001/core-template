package com.isoftstone.crawl.template.impl;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.isoftstone.crawl.template.itf.IIndexerHandler;

public class XpathIndexer implements IIndexerHandler {
	
	private static final Logger LOG =LoggerFactory.getLogger(XpathIndexer.class);
	private SelectorIndexer index = null;
	
	public XpathIndexer(){
		
	}
	
	public XpathIndexer(SelectorIndexer index){
		this.index=index;
	}
	
	@Override
	public ArrayList<String> index(byte[] input, String encoding, String url) {
		return null;
	}
	
	private ArrayList<String> attributorHandler(org.jsoup.select.Elements elements, String encoding) {
		return null;
	}
}
