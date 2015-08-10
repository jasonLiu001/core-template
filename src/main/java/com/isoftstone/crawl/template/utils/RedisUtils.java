package com.isoftstone.crawl.template.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import com.alibaba.fastjson.JSON;
import com.isoftstone.crawl.template.global.Constants;
import com.isoftstone.crawl.template.impl.ParseResult;
import com.isoftstone.crawl.template.impl.TemplateResult;
import com.isoftstone.crawl.template.vo.CrawlQueueItem;
import com.isoftstone.crawl.template.vo.DispatchVo;

public class RedisUtils {
	private static JedisPool jedisPool = null;
	private static PropertiesUtils propert = PropertiesUtils.getInstance();

	private static final Log LOG = LogFactory.getLog(RedisUtils.class);

	private static void initialPool() {
		try {
			if (jedisPool == null) {
				JedisPoolConfig config = new JedisPoolConfig();
				// 可用连接实例的最大数目，为负值时没有限制
				config.setMaxTotal(-1);
				// 空闲连接实例的最大数目，为负值时没有限制
				config.setMaxIdle(-1);
				// 表示当borrow(引入)一个jedis实例时，最大的等待时间，如果超过等待时间，则直接抛出JedisConnectionException；
				config.setMaxWaitMillis(1000 * 1000);
				// 当调用borrow Object方法时，是否进行有效性检查
				config.setTestOnBorrow(true);

				String ip = Constants.REDIS_IP;
				if (propert.getValue("template.redis.ip") != null)
					ip = propert.getValue("template.redis.ip");

				int port = Constants.REDIS_PORT;
				if (propert.getValue("template.redis.port") != null)
					port = Integer.parseInt(propert.getValue("template.redis.port"));

				jedisPool = new JedisPool(config, ip, port, 1000000);
			}
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}
	}

	/**
	 * 同步获取Jedis实例
	 * 
	 * @return Jedis
	 */
	public synchronized static Jedis getJedis() {
		if (jedisPool == null) {
			poolInit();
		}
		Jedis jedis = null;
		try {
			if (jedisPool != null) {
				jedis = jedisPool.getResource();
			}
		} catch (Exception e) {
			LOG.error("Get jedis error : " + e.getMessage());
		} finally {
			returnResource(jedis);
		}
		return jedis;
	}

	/**
	 * 在多线程环境同步初始化
	 */
	private static synchronized void poolInit() {
		if (jedisPool == null) {
			initialPool();
		}
	}

	public static void returnResource(Jedis jedis) {
		if (jedis != null && jedisPool != null) {
			jedisPool.returnResource(jedis);
		}
	}

	public static TemplateResult getTemplateResult(String guid) {
		Jedis jedis = null;
		try {
			jedis = getJedis();
			String json = jedis.get(guid);
			if (json != null)
				return JSONUtils.getTemplateResultObject(json);
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}
		return null;
	}

	public static TemplateResult getTemplateResult(String guid, int dbindex) {
		Jedis jedis = null;
		try {
			jedis = getJedis();
			jedis.select(dbindex);
			String json = jedis.get(guid);
			if (json != null)
				return JSONUtils.getTemplateResultObject(json);
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}
		return null;
	}

	public static void setTemplateResult(TemplateResult templateResult, String guid) {
		Jedis jedis = null;
		try {
			StringBuilder str = new StringBuilder();
			str.append(JSONUtils.getTemplateResultJSON(templateResult));
			jedis = getJedis();
			jedis.set(guid, str.toString());
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}
	}

	public static void setHtmlResult(String url, String html) {
		Jedis jedis = null;
		try {
			String guid = MD5Utils.MD5(url) + "_rawHtml";
			jedis = getJedis();
			jedis.select(Constants.RAWHTML_REDIS_DBINDEX);
			jedis.set(guid, html);
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}
	}

	public static void setHtmlResult(String url, byte[] html) {
		Jedis jedis = null;
		try {
			String guid = MD5Utils.MD5(url) + "_rawHtml";
			jedis = getJedis();
			jedis.select(Constants.RAWHTML_REDIS_DBINDEX);
			jedis.set(guid.getBytes(), html);
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}
	}

	public static byte[] getHtmlResultByte(String url) {
		Jedis jedis = null;
		try {
			String guid = MD5Utils.MD5(url) + "_rawHtml";
			jedis = getJedis();
			jedis.select(Constants.RAWHTML_REDIS_DBINDEX);
			byte[] json = jedis.get(guid.getBytes());
			if (json != null)
				return json;
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}
		return null;
	}

	public static String getHtmlResult(String url) {
		Jedis jedis = null;
		try {
			String guid = MD5Utils.MD5(url) + "_rawHtml";
			jedis = getJedis();
			jedis.select(Constants.RAWHTML_REDIS_DBINDEX);
			String json = jedis.get(guid);
			if (json != null)
				return json;
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}
		return null;
	}

	public static void setTemplateResult(TemplateResult templateResult, String guid, int dbindex) {
		Jedis jedis = null;
		try {
			StringBuilder str = new StringBuilder();
			str.append(JSONUtils.getTemplateResultJSON(templateResult));
			jedis = getJedis();
			jedis.select(dbindex);
			jedis.set(guid, str.toString());
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}
	}

	public static byte[] getByteArrayByJSONString(String jsonString) {
		byte[] byteArray = null;
		try {
			ObjectMapper objectmapper = new ObjectMapper();
			byteArray = objectmapper.readValue(jsonString, byte[].class);
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return byteArray;
	}

	public static void saveStr(String str, String guid, int dbindex) {
		Jedis jedis = null;
		try {
			jedis = getJedis();
			jedis.select(dbindex);
			jedis.set(guid, str);
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}
	}

	public static long remove(String guid) {
		Jedis jedis = null;
		try {
			jedis = getJedis();
			return jedis.del(guid);
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}
		return -1;
	}

	public static long remove(String guid, int dbindex) {
		Jedis jedis = null;
		try {
			jedis = getJedis();
			jedis.select(dbindex);
			return jedis.del(guid);
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}
		return -1;
	}

	public static ParseResult getParseResult(String guid, int dbindex) {
		Jedis jedis = null;
		try {
			jedis = getJedis();
			jedis.select(dbindex);
			// System.out.println("guid=" + guid);
			String json = jedis.get(guid);
			if (json != null && !json.isEmpty()) {
				return JSONUtils.getParseResultObject(json);
			}
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}
		return null;
	}

	public static ParseResult getParseResult(String guid) {
		Jedis jedis = null;
		try {
			jedis = getJedis();
			// System.out.println("guid=" + guid);
			String json = jedis.get(guid);
			// System.out.println("json=" + json);
			return JSONUtils.getParseResultObject(json);
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}
		return null;
	}

	public static void setParseResult(ParseResult parseResult, String guid, int dbindex) {
		Jedis jedis = null;
		try {
			StringBuilder str = new StringBuilder();
			str.append(JSONUtils.getParseResultJSON(parseResult));
			jedis = getJedis();
			jedis.select(dbindex);
			jedis.set(guid, str.toString());
			// jedis.expire(guid, Constants.REDIS_EXPIRE_TIME);
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}
	}

	public static boolean contains(String guid) {
		Jedis jedis = null;
		boolean flag = false;
		try {
			jedis = getJedis();
			flag = jedis.exists(guid);
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}
		return flag;
	}

	public static boolean contains(String guid, int dbindex) {
		Jedis jedis = null;
		boolean flag = false;
		try {
			jedis = getJedis();
			jedis.select(dbindex);
			flag = jedis.exists(guid);
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}
		return flag;
	}

	public static List<DispatchVo> getDispatchListResult(List<String> redisKeys, int dbIndex) {
		Jedis jedis = null;
		List<DispatchVo> result = new ArrayList<>();
		try {
			jedis = getJedis();
			jedis.select(dbIndex);
			List<String> json = jedis.mget(redisKeys.toArray(new String[0]));
			if (json != null) {
				for (String js : json)
					result.add(JSON.parseObject(js, DispatchVo.class));
			}
		} catch (Exception e) {
			LOG.error("get dispatch result from redis failed", e);
		}
		return result;
	}

	public static CrawlQueueItem getQueueItem() {
		Jedis jedis = null;
		try {
			jedis = getJedis();
			jedis.select(Constants.DEFAULT_REDIS_DBINDEX);
			List<String> json = jedis.brpop(Constants.REDIS_POP_TIMEOUT, Constants.REDIS_SCHEDULE_QUEUE);
			if (json != null && json.isEmpty() == false) {
				return JSON.parseObject(json.get(1), CrawlQueueItem.class);
			} else {
				return null;
			}
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}
		return null;
	}

	public static void setQueueItem(CrawlQueueItem item) {
		Jedis jedis = null;
		try {
			jedis = getJedis();
			jedis.select(Constants.DEFAULT_REDIS_DBINDEX);
			String json = JSON.toJSONString(item);
			jedis.lpush(Constants.REDIS_SCHEDULE_QUEUE, json);
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}
	}

	public static void main(String[] args) {
		for (int i = 0; i < 300; i++) {
			new Thread().start();
		}

		System.out.println(getTemplateResult("65E055987D23F142C8CAE7F9E975A09D", 0));
		System.out.println("~~~~~~~~~~");
		System.out.println(getTemplateResult("006AE4528A49E627595FB0474222924F", 0));
	}
}
