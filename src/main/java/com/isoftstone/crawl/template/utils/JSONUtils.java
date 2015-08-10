package com.isoftstone.crawl.template.utils;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.isoftstone.crawl.template.impl.ParseResult;
import com.isoftstone.crawl.template.impl.TemplateResult;

public class JSONUtils {
	public static TemplateResult getTemplateResultObject(String json) {
		TemplateResult templateResult = null;
		try {
			ObjectMapper objectmapper = new ObjectMapper();
			templateResult = objectmapper.readValue(json, TemplateResult.class);
		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return templateResult;
	}

	public static String getTemplateResultJSON(TemplateResult templateResult) {
		String json = null;

		ObjectMapper objectmapper = new ObjectMapper();
		try {
			json = objectmapper.writeValueAsString(templateResult);
		} catch (JsonGenerationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return json;
	}

	public static ParseResult getParseResultObject(String json) {
		ParseResult parseResult = null;
		try {
			ObjectMapper objectmapper = new ObjectMapper();
			parseResult = objectmapper.readValue(json, ParseResult.class);
		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return parseResult;
	}

	public static String getParseResultJSON(ParseResult parseResult) {
		String json = null;
		ObjectMapper objectmapper = new ObjectMapper();
		try {
			json = objectmapper.writeValueAsString(parseResult);
		} catch (JsonGenerationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return json;
	}
}
