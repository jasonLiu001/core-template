package com.isoftstone.crawl.template.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.isoftstone.crawl.template.global.Constants;
import com.isoftstone.crawl.template.utils.MD5Utils;
import com.isoftstone.crawl.template.utils.RedisUtils;

public class TemplateFactory {
	private static final Logger LOG = LoggerFactory.getLogger(TemplateFactory.class);

	public static ParseResult process(byte[] input, String encoding, String url, int dbindex) {
		String guid = MD5Utils.MD5(url);
		//System.out.println("guid:" + guid);
		List<Selector> selectors = null;
		ParseResult parseResult = null;
		TemplateResult templateResult = RedisUtils.getTemplateResult(guid, dbindex);
		if (templateResult != null) {
			String templateGuid = templateResult.getTemplateGuid();// 获取模板的GUID
			// System.out.println("templateGuid:" + guid);
			String type = templateResult.getType();// 获取当前页面的类型
			parseResult = new ParseResult();
			if (Constants.TEMPLATE_LIST.equals(type)) {// 列表页
				selectors = templateResult.getList();
				if (templateResult.getPagination() != null) {
					selectors.addAll(templateResult.getPagination());
				}
				// parseResult = new ParseResult();
			} else if (Constants.TEMPLATE_PAGITATION.equals(type)) {// 分页
				selectors = RedisUtils.getTemplateResult(templateGuid, dbindex)!=null?RedisUtils.getTemplateResult(templateGuid, dbindex).getList():null;
				// parseResult = new ParseResult();
			} else if (Constants.TEMPLATE_NEWS.equals(type)) {// 内容页
				selectors = RedisUtils.getTemplateResult(templateGuid, dbindex)!= null ?RedisUtils.getTemplateResult(templateGuid, dbindex).getNews():null;// 获取新闻页模板
				String parseResultGuid = templateResult.getParseResultGuid(); // 获取中间结果
				parseResult = RedisUtils.getParseResult(parseResultGuid, dbindex);
				if (parseResult == null)// 中间结果如果为null，则new ParseResult()
				{
					parseResult = new ParseResult();
				}
				if (templateResult.getTags() != null) {
					parseResult.setResult(Constants.TEMPLATE_STATIC, templateResult.getTags().toString());
				}
			} else {
				LOG.error("This page " + url + " ， the type " + templateResult.getType() + " , it isn't defined in the Template.");
				return null;
			}
			int flag = 0;
			if (selectors != null) {
				parseResult.setTemplateGuid(templateGuid);
				for (Selector selector : selectors) {
					flag = selector.process(input, encoding, url, parseResult);
					if (flag == -1) {
						break;
					}
				}
			} else {
				LOG.error("This page " + url + " ， the type " + templateResult.getType() + " , it isn't defined in the Template.");
				return null;
			}
			if (flag != -1) {
				if (Constants.TEMPLATE_LIST.equals(type)) {
					saveOutlinkParseResultToRedis(parseResult, templateGuid, dbindex);
				} else if (Constants.TEMPLATE_NEWS.equals(type)) {
					 RedisUtils.remove(templateResult.getParseResultGuid(),dbindex);
				}
			}

		} else {
			LOG.error("This page " + url + " ， not found the Template.");
		}
		return parseResult;
	}

	public static ParseResult process(byte[] input, String encoding, String url) {
		String guid = MD5Utils.MD5(url);
		// System.out.println("guid:" + guid);
		List<Selector> selectors = null;
		ParseResult parseResult = null;
		TemplateResult templateResult = RedisUtils.getTemplateResult(guid);
		if (templateResult != null) {
			String templateGuid = templateResult.getTemplateGuid();// 获取模板的GUID
			// System.out.println("templateGuid:" + guid);
			String type = templateResult.getType();// 获取当前页面的类型
			if (Constants.TEMPLATE_LIST.equals(type)) {// 列表页
				selectors = templateResult.getList();
				if (templateResult.getPagination() != null) {
					selectors.addAll(templateResult.getPagination());
				}
				parseResult = new ParseResult();
			} else if (Constants.TEMPLATE_PAGITATION.equals(type)) {// 分页
				selectors = RedisUtils.getTemplateResult(templateGuid).getList();
				parseResult = new ParseResult();
			} else if (Constants.TEMPLATE_NEWS.equals(type)) {// 内容页
				selectors = RedisUtils.getTemplateResult(templateGuid).getNews();// 获取新闻页模板
				String parseResultGuid = templateResult.getParseResultGuid(); // 获取中间结果
				parseResult = RedisUtils.getParseResult(parseResultGuid);
				if (templateResult.getTags() != null) {
					parseResult.setResult(Constants.TEMPLATE_STATIC, templateResult.getTags().toString());
				}
			} else {
				LOG.error("This page " + url + " ， the type " + templateResult.getType() + " , it isn't defined in the Template.");
				return null;
			}

			int flag = 0;
			if (selectors != null) {
				parseResult.setTemplateGuid(templateGuid);
				for (Selector selector : selectors) {
					flag = selector.process(input, encoding, url, parseResult);
					if (flag == -1) {
						break;
					}
				}
			} else {
				LOG.error("This page " + url + " ， the type " + templateResult.getType() + " , it isn't defined in the Template.");
				return null;
			}
			if (flag != -1) {
				if (Constants.TEMPLATE_LIST.equals(type)) {
					saveOutlinkParseResultToRedis(parseResult, templateGuid, 0);
				} else if (Constants.TEMPLATE_NEWS.equals(type)) {
					RedisUtils.remove(templateResult.getParseResultGuid());
				}
			}
		} else {
			LOG.error("This page " + url + " ， not found the Template.");
		}
		return parseResult;
	}

	/**
	 * 
	 * @Title: isUpdate
	 * @Description: TODO(比较网页签名判断网页是否更新)
	 * @param @param signature 原网页签名
	 * @param @param input 当前网页源码
	 * @param @return 设定文件
	 * @return boolean 返回类型
	 * @author lj
	 * @throws
	 */
	public static boolean isUpdate(String signature, byte[] input) {
		if (signature != null) {
			if (!signature.equals(MD5Utils.MD5(input))) {// 如不相等，网页更新，解析之
				return true;
			}
		}
		return false;
	}

	public static ParseResult localProcess(byte[] input, String encoding, String url, TemplateResult templateResult, String pageType) {
		List<Selector> selectors = null;
		ParseResult parseResult = new ParseResult();
		String templateGuid = templateResult.getTemplateGuid();

		if (Constants.TEMPLATE_LIST.equals(pageType)) {
			selectors = templateResult.getList();
			if (selectors == null) {
				LOG.error("This page " + url + " ， the type " + templateResult.getType() + " , it isn't defined in the Template.");
				return null;
			}
			if (templateResult.getPagination() != null) {
				selectors.addAll(templateResult.getPagination());
			}
		} else if (Constants.TEMPLATE_PAGITATION.equals(pageType)) {
			selectors = templateResult.getList();
		} else if (Constants.TEMPLATE_NEWS.equals(pageType)) {
			// 获取新闻页模板
			selectors = templateResult.getNews();
		} else {
			LOG.error("This page " + url + " ， the type " + templateResult.getType() + " , it isn't defined in the Template.");
			return null;
		}
		parseResult.setTemplateGuid(templateGuid);
		if (selectors != null) {
			for (Selector selector : selectors) {
				selector.process(input, encoding, url, parseResult);
			}
		}
		return parseResult;
	}

	public static ArrayList<String> getContentOutlink(ParseResult parseResult) {
		ArrayList<String> result = null;
		if (parseResult != null) {
			Map<String, String> results = parseResult.getResult();
			if (results != null && results.size() > 0) {
				if (results.containsKey(Constants.CONTENT_OUTLINK)) {
					result = new ArrayList<String>();
					int len = Integer.parseInt(results.get(Constants.CONTENT_OUTLINK));
					for (int i = 0; i < len; i++) {
						String outlink = results.get(Constants.CONTENT_OUTLINK + "_" + i);
						result.add(outlink);
					}
				}
			}
		}
		return result;
	}

	public static ArrayList<String> getPaginationOutlink(ParseResult parseResult) {
		ArrayList<String> result = null;
		if (parseResult != null) {
			Map<String, String> results = parseResult.getResult();
			if (results != null && results.size() > 0) {
				if (results.containsKey(Constants.PAGINATION_OUTLINK)) {
					result = new ArrayList<String>();
					int len = Integer.parseInt(results.get(Constants.PAGINATION_OUTLINK));
					Set<String> keySet = results.keySet();
					for (int i = 0; i < len; i++) {
						if (keySet.contains(Constants.PAGINATION_OUTLINK + "_" + i)) {
							String outlink = results.get(Constants.PAGINATION_OUTLINK + "_" + i);
							result.add(outlink);
						} else {
							continue;
						}
					}
				}
			}
		}
		return result;
	}

	public static ArrayList<String> getOutlink(ParseResult parseResult) {
		if (parseResult != null) {
			ArrayList<String> result = new ArrayList<String>();
			ArrayList<String> contents = getContentOutlink(parseResult);
			if (contents != null && contents.size() > 0) {
				result.addAll(contents);
			}
			ArrayList<String> pagination = getPaginationOutlink(parseResult);
			if (pagination != null && pagination.size() > 0) {
				result.addAll(pagination);
			}
			return result;
		}
		return null;
	}

	private static void saveOutlinkParseResultToRedis(ParseResult parseResult, String templateGuid, int dbindex) {
		Map<String, String> hash = parseResult.getResult();
		HashMap<String, String> tags = RedisUtils.getTemplateResult(templateGuid, dbindex).getTags();
		String contents = null;
		if (hash.keySet().contains(Constants.CONTENT_OUTLINK)) {
			contents = hash.get(Constants.CONTENT_OUTLINK);
		}
		int contentlength = 0;
		if (contents != null && !contents.isEmpty()) {
			String labels = hash.get(Constants.LABEL_LIST);
			String labelKeys[] = null;
			if (labels != null && !labels.isEmpty()) {
				labels = labels.substring(0, labels.length() - 1);
				labelKeys = labels.split(",");
			}
			contentlength = Integer.parseInt(contents);
			if (labelKeys != null && labelKeys.length > 0) {
				for (int i = 0; i < contentlength; i++) {
					String outlink = hash.get(Constants.CONTENT_OUTLINK + "_" + i);
					if (outlink != null && !outlink.isEmpty()) {
						TemplateResult t = new TemplateResult();
						String guid = MD5Utils.MD5(outlink);
						String parseResultGuid = guid + Constants.PARSE_RESULT_PREFIX;
						t.setType(Constants.TEMPLATE_NEWS);
						t.setTags(tags);
						t.setState(Constants.UN_FETCH);
						t.setTemplateGuid(templateGuid);
						t.setParseResultGuid(parseResultGuid);
						ParseResult r = new ParseResult();
						r.setResult(Constants.ARTICLE_URL, outlink);
						for (int j = 0; j < labelKeys.length; j++) {
							String key = labelKeys[j];
							String value = hash.get(key + "_" + i);
							if (value != null && !value.isEmpty()) {
								r.setResult(key, value);
							}
						}
						RedisUtils.setParseResult(r, parseResultGuid, dbindex);
						RedisUtils.setTemplateResult(t, guid, dbindex);
					}
				}
			} else {
				for (int i = 0; i < contentlength; i++) {
					String outlink = hash.get(Constants.CONTENT_OUTLINK + "_" + i);
					if (outlink != null && !outlink.isEmpty()) {
						TemplateResult t = new TemplateResult();
						String guid = MD5Utils.MD5(outlink);
						String parseResultGuid = guid + Constants.PARSE_RESULT_PREFIX;
						t.setType(Constants.TEMPLATE_NEWS);
						t.setTags(tags);
						t.setState(Constants.UN_FETCH);
						t.setTemplateGuid(templateGuid);
						t.setParseResultGuid(parseResultGuid);
						ParseResult r = new ParseResult();
						r.setResult(Constants.ARTICLE_URL, outlink);
						RedisUtils.setParseResult(r, parseResultGuid, dbindex);
						RedisUtils.setTemplateResult(t, guid, dbindex);
					}
				}
			}
		}
		String pagitations = null;
		if (hash.keySet().contains(Constants.PAGINATION_OUTLINK)) {
			pagitations = hash.get(Constants.PAGINATION_OUTLINK);
		}
		int pagitationLength = 0;
		if (pagitations != null && !pagitations.isEmpty()) {
			pagitationLength = Integer.parseInt(pagitations);
			TemplateResult rootTemplate = RedisUtils.getTemplateResult(templateGuid, dbindex);
			for (int i = 0; i < pagitationLength; i++) {
				String outlink = hash.get(Constants.PAGINATION_OUTLINK + "_" + i);
				if (outlink != null && !outlink.isEmpty()) {
					TemplateResult t = new TemplateResult();
					String guid = MD5Utils.MD5(outlink);
					t.setType(Constants.TEMPLATE_LIST);
					t.setTags(tags);
					t.setState(Constants.UN_FETCH);
					t.setList(rootTemplate.getList());
					t.setTemplateGuid(templateGuid);
					RedisUtils.setTemplateResult(t, guid, dbindex);
				}
			}
		}
	}
}
