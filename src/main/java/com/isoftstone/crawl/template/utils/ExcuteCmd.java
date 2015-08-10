package com.isoftstone.crawl.template.utils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ExcuteCmd {
	private static final Log LOG = LogFactory.getLog(ExcuteCmd.class);

	/**
	 * @Title: excuteCmd
	 * @Description: (执行系统角本)
	 * @param @param cmd 设定文件
	 * @return int 返回类型
	 * @author lj
	 * @throws
	 */
	public static int excuteCmd(String cmd) {
		int exitVal = -1;
		try {
			LOG.info("#shell:" + cmd);
			Runtime rt = Runtime.getRuntime();
			Process proc = rt.exec(cmd);
			InputStream stdin = proc.getInputStream();
			InputStreamReader isr = new InputStreamReader(stdin);
			BufferedReader br = new BufferedReader(isr);
			String line = null;
			while ((line = br.readLine()) != null)
				LOG.info(line);
			exitVal = proc.waitFor();
			LOG.info("Process exitValue:" + exitVal);
		} catch (Throwable t) {
			t.printStackTrace();
		}
		return exitVal;
	}

	/**
	 * @Title: excuteCmd
	 * @Description: (执行系统角本)
	 * @param @param cmd 设定文件
	 * @return int 执行状态 0=正常 其它=异常
	 * @author lj
	 * @throws
	 */
	public static int excuteCmd(String[] cmds) {
		int exitVal = -1;
		try {
			String cmd = "";
			for (String str : cmds) {
				cmd += str + " ";
			}
			LOG.info("shell command:"+ cmd );
			Runtime rt = Runtime.getRuntime();
			Process proc = rt.exec(cmds);
			InputStream stdin = proc.getInputStream();
			InputStreamReader isr = new InputStreamReader(stdin);
			BufferedReader br = new BufferedReader(isr);
			String line = null;
			while ((line = br.readLine()) != null)
				LOG.info(line);
			exitVal = proc.waitFor();
			LOG.info("Process exitValue:" + exitVal);
		} catch (Throwable t) {
			t.printStackTrace();
		}
		return exitVal;
	}

}
