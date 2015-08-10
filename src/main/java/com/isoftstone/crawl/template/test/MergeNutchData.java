package com.isoftstone.crawl.template.test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.isoftstone.crawl.template.utils.ExcuteCmd;

/**
 * Created by Administrator on 2015/4/22.
 */
public class MergeNutchData {
	private static final Log LOG = LogFactory.getLog(MergeNutchData.class);
	private final String MERGE_CRAWLDB = " mergedb ";
	private final String MERGE_LINKDB = " mergelinkdb ";
	private final String MERGE_SEGMENTS = " mergesegs ";

	private final String CRAWLDB = "crawldb";
	private final String LINKDB = "linkdb";
	private final String SEGMENTS = "segments";

	private final String RM = "rm -rf %s";
	private final String MV = "mv -f %s %s";
	private final String MKDIR = "mkdir %s";

	private final String SUFFIX_MERGED = "_merged";
	private final String SUFFIX_BACKUP = "_backup";

	private List<String> data_list = Arrays.asList("crawldb", "linkdb", "segments");
	private String nutch_root = "/nutch_run/local_incremental/bin/nutch";
	private String output_folder = "/nutch_final_data/";
	private String data_folder = "/nutch_data/";
	private String temp_folder = "/nutch_temp_data/";
	private String market_folder = "/nutch_market_data/";

	public MergeNutchData(String nutch_root, String output_folder, String data_folder) {
		this.nutch_root = nutch_root;
		this.output_folder = output_folder;
		this.data_folder = data_folder;
	}

	public static void main(String[] args) {
		int length = args.length;
		String nutch_root;
		String output_folder;
		String data_folder;
		String data_domain;
		if (length < 3) {// 参数不全,返回
			System.err.println("Usage: MergeNutchData <nutch_root> <output_folder> <data_folder> [data_domain]");
			System.err.println("nutch_root		nutch shell executable directory ");
			System.err.println("output_folder		merged output_folder");
			System.err.println("data_folder		nutch_data folder");
			System.err.println("data_domain		optional - if data_domain empty, default merge all domain");
			return;
		} else {
			nutch_root = args[0];
			output_folder = args[1];
			data_folder = args[2];
			MergeNutchData merge = new MergeNutchData(nutch_root, output_folder, data_folder);
			switch (length) {
			case 4:// 参数为4个时，直接处理
				data_domain = args[3];
				if (nutch_root != null && output_folder != null && data_folder != null && data_domain != null) {
					LOG.info("MergeNutchData nutch_root: " + nutch_root);
					LOG.info("MergeNutchData output_folder: " + output_folder);
					LOG.info("MergeNutchData data_folder: " + data_folder);
					LOG.info("MergeNutchData data_domain: " + data_domain);
				}
				LOG.info("~~~~~~~~~~~~ MergeNutchData " + data_domain + " start ~~~~~~~~~~~~~~~~~~~~");
				merge.mergeByDomain(data_domain);
				break;
			case 3:// 参数为3个时，处理指定data_folder下所有的domain
				LOG.info("~~~~~~~~~~~~ MergeNutchData all domain start ~~~~~~~~~~~~~~~~~~~~");
				for (String str : merge.getDomainList(data_folder)) {
					// System.out.println(str);
					merge.mergeByDomain(str);
				}
				break;
			default:
				break;
			}
		}
	}

	// getDomainList
	public List<String> getDomainList(String data_folder) {
		List<String> ls = new ArrayList<String>();
		try {
			File[] folders = new File(data_folder).listFiles();
			if (folders != null && folders.length > 0) {
				for (File folder : folders) {
					String fname = folder.getName();
					String host = fname.substring(0, fname.indexOf("_"));// 提取host
					if (!ls.contains(host)) {
						ls.add(host);
						// System.out.println(host);
					}
				}
			}
		} catch (Exception e) {
			LOG.info("getDomainList:" + e.getMessage());
		}
		return ls;
	}

	public void mergeByDomain(String domain) {
		try {
			List<String> ls = new ArrayList<String>();
			File[] folders = new File(data_folder).listFiles();
			for (String data_name : data_list) {// 分别处理各data目录[crawldb、linkdb、segments]
				for (File folder : folders) {
					try {
						File f = null;
						String fname = folder.getName();
						String host = fname.substring(0, fname.indexOf("_"));// 提取host
						if (host.equals(domain)) {
							f = new File(data_folder + "/" + fname + "/" + data_name);
							if (f.exists()) {
								if (data_name.equals(SEGMENTS)) {
									mergeSegments(f.getPath(), domain);
								}
								ls.add(f.getPath());
							} else {
								LOG.info(fname + "/" + data_name + " directory not found!");
							}

							if (ls.size() == 10)// 防止一次合并过多,一次最多合并10个
							{
								switch (data_name) {
								case CRAWLDB:// 1、crawldb
									mergeCrawlDB(ls, domain);
									break;
								case LINKDB:// 2、linkdb
									mergeLinkDB(ls, domain);
									break;
								default:
									break;
								}
								ls.clear();
							}
						}
					} catch (Exception e) {
						LOG.error("mergeByDomain:" + e.getMessage());
					}
				}
				if (ls != null && ls.size() > 0) {
					// 不足5个,有多少处理多少
					switch (data_name) {
					case CRAWLDB:// 1、crawldb
						mergeCrawlDB(ls, domain);
						// Crawldbs 归档
						normalizingCrawldbs(domain);
						ls.clear();
						break;
					case LINKDB:// 2、linkdb
						mergeLinkDB(ls, domain);
						// Linkdbs 归档
						normalizingLinkdbs(domain);
						ls.clear();
						break;
					case SEGMENTS:
						// Segments 归档
						normalizingSegments(domain);
						break;
					default:
						break;
					}
				}
			}
		} catch (Exception e) {
			LOG.error("mergeByDomain:" + e.getMessage());
		}
	}

	// MergeCrawlDB
	public void mergeCrawlDB(List<String> crawldb_list, String domain) {
		if (nutch_root.length() > 0 && nutch_root != null && crawldb_list != null) {
			String merge_crawldb = nutch_root + MERGE_CRAWLDB + market_folder + domain + "_data/crawldbs/" + System.currentTimeMillis() + "/" + CRAWLDB + " %s";
			String[] cmds;
			try {
				StringBuilder sb = new StringBuilder();
				if (crawldb_list != null) {
					for (String crawldb_folder : crawldb_list) {
						sb.append(crawldb_folder);
						sb.append(" ");
					}
					String folderStr = sb.deleteCharAt(sb.length() - 1).toString();
					cmds = new String[] { "/bin/sh", "-c", String.format(merge_crawldb, folderStr) };
					if (ExcuteCmd.excuteCmd(cmds) == 0)// merge crawldb succeed
					{
						cmds = new String[] { "/bin/sh", "-c", String.format(RM, folderStr) };
						ExcuteCmd.excuteCmd(cmds); // 删除目录
						LOG.info("======================================================");
					}
				}
			} catch (Exception e) {
				LOG.error("merge crawldb:" + e.getMessage());
			}
		}
	}

	// Crawldb 归档
	public void normalizingCrawldbs(String domain) {
		String[] cmds;
		String fina_data_crawldb = output_folder + domain + "/crawldb/";
		String market_data = market_folder + domain + "_data/crawldbs/";
		String merge_crawldb = nutch_root + MERGE_CRAWLDB + fina_data_crawldb + " %s";
		try {
			File f = new File(fina_data_crawldb);
			if (f.exists())// 移动到market_folder
			{
				cmds = new String[] { "/bin/sh", "-c", String.format(MV, fina_data_crawldb + "*", market_data + "0000000000000/" + CRAWLDB + "/") };
				ExcuteCmd.excuteCmd(cmds);
				cmds = new String[] { "/bin/sh", "-c", String.format(RM, fina_data_crawldb) };
				ExcuteCmd.excuteCmd(cmds);
			}

			File[] folders = new File(market_data).listFiles();// 所有待合并的crawldb
			StringBuilder sb = new StringBuilder();
			for (File crawldb_folder : folders) {
				sb.append(crawldb_folder.getPath() + "/" + CRAWLDB);
				sb.append(" ");
			}
			String folderStr = sb.deleteCharAt(sb.length() - 1).toString();
			cmds = new String[] { "/bin/sh", "-c", String.format(merge_crawldb, folderStr) };
			if (ExcuteCmd.excuteCmd(cmds) == 0)// merge crawldb succeed
			{
				cmds = new String[] { "/bin/sh", "-c", String.format(RM, market_data) };// 合并成功后删除原目录
				if (ExcuteCmd.excuteCmd(cmds) == 0) {
					LOG.info("======================================================");
				}
			}
		} catch (Exception e) {
			LOG.error("normalizingCrawldbs error:" + e.getMessage());
		}

	}

	// MergeLinkDB
	public void mergeLinkDB(List<String> linkdb_list, String domain) {
		if (nutch_root.length() > 0 && nutch_root != null && linkdb_list != null) {
			String merge_linkdb = nutch_root + MERGE_LINKDB + market_folder + domain + "_data/linkdbs/" + System.currentTimeMillis() + "/" + LINKDB + " %s";
			String[] cmds;
			try {
				if (linkdb_list != null) {
					StringBuilder sb = new StringBuilder();
					for (String linkdb_folder : linkdb_list) {
						sb.append(linkdb_folder);
						sb.append(" ");
					}
					String folderStr = sb.deleteCharAt(sb.length() - 1).toString();
					cmds = new String[] { "/bin/sh", "-c", String.format(merge_linkdb, folderStr) };
					if (ExcuteCmd.excuteCmd(cmds) == 0) {// merge linkdb succeed
						cmds = new String[] { "/bin/sh", "-c", String.format(RM, folderStr) };
						ExcuteCmd.excuteCmd(cmds); // 删除目录
						LOG.info("============================================================");
					}
				}

			} catch (Exception e) {
				LOG.error("merge linkdb:" + e.getMessage());
			}
		}
	}

	// Linkdb 归档
	public void normalizingLinkdbs(String domain) {
		String[] cmds;
		String fina_data_linkdb = output_folder + domain + "/linkdb/";
		String market_data = market_folder + domain + "_data/linkdbs/";
		String merge_linkdb = nutch_root + MERGE_LINKDB + fina_data_linkdb + " %s";
		try {
			File f = new File(fina_data_linkdb);
			if (f.exists())// 移动到market_folder
			{
				cmds = new String[] { "/bin/sh", "-c", String.format(MV, fina_data_linkdb + "*", market_data + "0000000000000/" + LINKDB + "/") };
				ExcuteCmd.excuteCmd(cmds);
				cmds = new String[] { "/bin/sh", "-c", String.format(RM, fina_data_linkdb) };
				ExcuteCmd.excuteCmd(cmds);
			}

			File[] folders = new File(market_data).listFiles();// 所有待合并的linkdb
			StringBuilder sb = new StringBuilder();
			for (File linkdb_folder : folders) {
				sb.append(linkdb_folder.getPath() + "/" + LINKDB);
				sb.append(" ");
			}
			String folderStr = sb.deleteCharAt(sb.length() - 1).toString();
			cmds = new String[] { "/bin/sh", "-c", String.format(merge_linkdb, folderStr) };
			if (ExcuteCmd.excuteCmd(cmds) == 0)// merge crawldb succeed
			{
				cmds = new String[] { "/bin/sh", "-c", String.format(RM, market_data) };// 合并成功后删除原目录
				if (ExcuteCmd.excuteCmd(cmds) == 0) {
					LOG.info("============================================================");
				}
			}
		} catch (Exception e) {
			LOG.error("normalizingLinkdbs error:" + e.getMessage());
		}
	}

	// MergeSegments
	public void mergeSegments(String segments_folder, String domain) {
		if (nutch_root.length() > 0 && nutch_root != null && segments_folder != null) {
			String[] cmds;
			String out_folder = temp_folder + domain + "_data_" + System.currentTimeMillis();
			String merge_segments = nutch_root + MERGE_SEGMENTS + out_folder + " -dir %s/";
			try {
				cmds = new String[] { "/bin/sh", "-c", String.format(merge_segments, segments_folder) };
				if (ExcuteCmd.excuteCmd(cmds) == 0) {// 合并Segments成功
					String data_folder = segments_folder.substring(0, segments_folder.indexOf(SEGMENTS));
					File crawldb = new File(data_folder + CRAWLDB);
					File linkdb = new File(data_folder + LINKDB);
					if (!crawldb.exists() && !linkdb.exists())// crawldb,linkdb不存在,证明两者均合并成功,删除整个目录
					{
						cmds = new String[] { "/bin/sh", "-c", String.format(RM, data_folder) };
						ExcuteCmd.excuteCmd(cmds);
					} else {
						cmds = new String[] { "/bin/sh", "-c", String.format(RM, segments_folder) };
						ExcuteCmd.excuteCmd(cmds);// 删除segments
					}

					String domain_folder = market_folder + domain + "_data/segments/";
					File f = new File(domain_folder);
					if (!f.exists())// 创建domain folder
					{
						f.mkdir();
						LOG.info("mkdir domain folder: " + f.getPath());
					}
					cmds = new String[] { "/bin/sh", "-c", String.format(MV, out_folder + "/*", f.getPath() + "/") };
					if (ExcuteCmd.excuteCmd(cmds) == 0) {// 将合并后的目录归档
						cmds = new String[] { "/bin/sh", "-c", String.format(RM, out_folder) };
						if (ExcuteCmd.excuteCmd(cmds) == 0) {// 删除临时目录
							LOG.info("============================================================");
						}
					}
				}
			} catch (Exception e) {
				LOG.error("merge segment:" + e.getMessage());
			}
		}
	}

	// Segments 归档
	public void normalizingSegments(String domain) {
		String fina_data_segments = output_folder + domain + "/segments/";
		String temp_data = market_folder + domain + "_data/segments/";
		String[] cmds;
		try {
			File f = new File(fina_data_segments);
			if (f.exists())// 移动到market_folder
			{
				cmds = new String[] { "/bin/sh", "-c", String.format(MV, fina_data_segments + "*", temp_data) };
				ExcuteCmd.excuteCmd(cmds); // 将final目录下的segments移动到临时目录下
				cmds = new String[] { "/bin/sh", "-c", String.format(RM, fina_data_segments) };
				ExcuteCmd.excuteCmd(cmds);
			}
			cmds = new String[] { "/bin/sh", "-c", nutch_root + MERGE_SEGMENTS + fina_data_segments + " -dir " + temp_data };
			if (ExcuteCmd.excuteCmd(cmds) == 0) {
				cmds = new String[] { "/bin/sh", "-c", String.format(RM, temp_data) };
				ExcuteCmd.excuteCmd(cmds);// 删除中转目录
				LOG.info("============normalizingSegments succeed=================");
			}
		} catch (Exception e) {
			LOG.error("normalizingSegments:" + e.getMessage());
		}
	}

	public void merge(String segments_folder, String domain) {
		if (nutch_root.length() > 0 && nutch_root != null && segments_folder != null) {
			String merge_segments = nutch_root + MERGE_SEGMENTS + segments_folder + SUFFIX_MERGED + " %s/*";
			try {
				String cmd = String.format(merge_segments, segments_folder);
				// System.out.println(cmd);
				if (ExcuteCmd.excuteCmd(cmd) == 0) {// merge segment succeed
					LOG.info("mergesegs command:" + cmd);
					cmd = String.format(RM, segments_folder + SUFFIX_BACKUP);
					// System.out.println(cmd);
					if (ExcuteCmd.excuteCmd(cmd) == 0)// 删除备份
					{
						LOG.info(cmd);
						cmd = String.format(MV, segments_folder, segments_folder + SUFFIX_BACKUP);
						// System.out.println(cmd);
						if (ExcuteCmd.excuteCmd(cmd) == 0)// 重命名
						{
							LOG.info(cmd);
							cmd = String.format(MKDIR, segments_folder);
							// System.out.println(cmd);
							if (ExcuteCmd.excuteCmd(cmd) == 0)// 创建segments
							{
								LOG.info(cmd);
								cmd = String.format(MV, segments_folder + SUFFIX_MERGED, segments_folder);
								// System.out.println(cmd);
								if (ExcuteCmd.excuteCmd(cmd) == 0)// meger_segments重命为segments
								{
									LOG.info(cmd);
									cmd = String.format(RM, segments_folder + SUFFIX_MERGED);// 删除合并结果
									// System.out.println(cmd);
									if (ExcuteCmd.excuteCmd(cmd) == 0) {
										LOG.info(cmd);
									}
								}
							}
						}
					}
				}
			} catch (Exception e) {
				LOG.error("merge segment:" + e.getMessage());
			}
		}
	}
}
