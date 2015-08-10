package com.isoftstone.crawl.template.itf;

import com.isoftstone.crawl.template.impl.ParseResult;

public interface ISelectorHandler {
	public int select(byte[] input, String encoding, String url,
			ParseResult parseResult);
}
