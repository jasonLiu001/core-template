package com.isoftstone.crawl.template.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
* @ClassName: Property 
* @Description: TODO(配置文件操作类) 
* @author lj
* @date 2014年7月25日 下午12:42:55 
*
 */
public class PropertiesUtils {
	private static Properties props = null;
	private static PropertiesUtils instance=null;
	private static final String properties_fileName = "template.properties";
	private static final Logger LOG = LoggerFactory.getLogger(PropertiesUtils.class);
	
	public static synchronized PropertiesUtils getInstance(String fileName){
		if(instance==null){
			if(!fileName.isEmpty() && fileName!=null)
				instance=new PropertiesUtils(fileName);
		}
		return instance;
	}
	
	public static synchronized PropertiesUtils getInstance(){
		if(instance==null){
			instance=new PropertiesUtils(properties_fileName);
		}
		return instance;
	}
	private PropertiesUtils(String fileName){
		init(fileName);
	}

	/**
	 * 读取属性完成初始化
	 */
	private void init(String fileName) {

		InputStream fileinputstream = null;
		try {
			fileinputstream = new FileInputStream(fileName);
		} catch (FileNotFoundException e1) {
			//LOG.info(e1.getMessage());
		}
		if (fileinputstream == null) {
			fileinputstream = PropertiesUtils.class.getClassLoader().getResourceAsStream(fileName);
			//LOG.info("InputStream 未找到"+fileName+"文件,改使用class.getResourceAsStream加载");
		}
		if(fileinputstream!= null)
		{
			props = new Properties();
			try {
				props.load(fileinputstream);
				//LOG.info(fileName + "加载成功");
			} catch (Exception e) {
				e.printStackTrace();
				LOG.info("不能读取属性文件. " + "请确保"+fileName+"在CLASSPATH指定的路径中");
				return;
			}
		}else
		{
			LOG.info(fileName +"未发现.");
		}
		
	}
	
	public String getValue(String param){
		return props.get(param)==null?null:String.valueOf(props.get(param)).trim();
	}
	
	public int getIntValue(String param){
		int value=0;
		Object obj=props.get(param);
		if(obj!=null){
			try{
				String temp=obj.toString();
				value=Integer.valueOf(temp.trim());
			}catch(Exception ex){
				ex.printStackTrace();
			}
		}
		return value;
	}
	public long getLongValue(String param){
		long value=0;
		Object obj=props.get(param);
		if(obj!=null){
			try{
				String temp=obj.toString();
				value=Long.valueOf(temp.trim());
			}catch(Exception ex){
				ex.printStackTrace();
			}
		}
		return value;
	}
	public boolean getBooleanValue(String param){
		boolean value=false;
		Object obj=props.get(param);
		if(obj!=null){
			try{
				String temp=obj.toString();
				value=Boolean.valueOf(temp.trim());
			}catch(Exception ex){
				ex.printStackTrace();
			}
		}
		return value;
	}
}








