package com.isoftstone.crawl.template.utils;

import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @ClassName: StringUtil
 * @Description: TODO 此类中收集Java编程中WEB开发常用到的一些工具。 为避免生成此类的实例，构造方法被申明为private类型的。
 * @author lj
 * @date 2014年10月22日 上午11:00:42
 */
public class StringUtil {
	/**
	 * 私有构造方法，防止类的实例化，因为工具类不需要实例化。
	 */
	private StringUtil() {
	}

	/**
	 * ASCII表中可见字符从!开始，偏移位值为33(Decimal)
	 */
	private static final char DBC_CHAR_START = 33; // 半角!
	/**
	 * ASCII表中可见字符到~结束，偏移位值为126(Decimal)
	 */
	private static final char DBC_CHAR_END = 126; // 半角~
	/**
	 * 全角对应于ASCII表的可见字符从！开始，偏移值为65281
	 */
	private static final char SBC_CHAR_START = 65281; // 全角！
	/**
	 * 全角对应于ASCII表的可见字符到～结束，偏移值为65374
	 */
	private static final char SBC_CHAR_END = 65374; // 全角～
	/**
	 * ASCII表中除空格外的可见字符与对应的全角字符的相对偏移
	 */
	private static final int CONVERT_STEP = 65248; // 全角半角转换间隔
	/**
	 * 全角空格的值，它没有遵从与ASCII的相对偏移，必须单独处理
	 */
	private static final char SBC_SPACE = 12288; // 全角空格 12288
	/**
	 * 半角空格的值，在ASCII中为32(Decimal)
	 */
	private static final char DBC_SPACE = ' '; // 半角空格
	private static final String REGEX_SCRIPT = "<script[^>]*?>[\\s\\S]*?<\\/script";// 定义script的正则表达
	private static final String REGEX_STYLE = "<style[^>]*?>[\\s\\S]*?<\\/style>"; // 定义style的正则表达式
	private static final String REGEX_HTML = "<[^>]+>|[<>]"; // 定义HTML标签的正则表达式
	// private static final String REGEX_TEMP =
	// "[`~!@＃#$%^&*()+=|{}';'\\[\\].<>/?~！@#￥%……&*（）——+|{}【】‘；”“\"\"’。，、？]"; //
	// 定义特殊字符
	private static final String REGEX_SPACE = "[\\s+| +|　+]";
	private static final String REG_IP = "(?:(?:25[0-5]|2[0-4]\\d|[01]?\\d?\\d)\\.){3}(?:25[0-5]|2[0-4]\\d|[01]?\\d?\\d)";
	private static final String regEx = "[\u4e00-\u9fa5]";

	private static final Pattern pat = initPattern(regEx);
	private static final Pattern p_ip = initPattern(REG_IP);

	private static final String[] imgSuffix = { "jpg", "jpeg", "png" };

	/**
	 * 检查图片url合法
	 * 
	 * @author lj
	 * @param 图片URL地址
	 * */
	public static boolean checkImgUrl(String url) {
		if (url != null && url.length() > 1) {
			String tmp = url.trim().toLowerCase();
			String suffix = tmp.substring(tmp.lastIndexOf(".") + 1, tmp.length());
			for (int i = 0; i < imgSuffix.length; i++) {
				if (imgSuffix[i].equals(suffix)) {
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * @param inputString
	 *            原始字符串 去除HTML标记
	 */
	public static String html2Text(String inputString) {
		if (!inputString.isEmpty() || inputString != null) {
			try {
				// 过滤script标签
				Pattern p_script = Pattern.compile(REGEX_SCRIPT, Pattern.CASE_INSENSITIVE);
				Matcher m_script = p_script.matcher(inputString);
				inputString = m_script.replaceAll("");
				// 过滤style标签
				Pattern p_style = Pattern.compile(REGEX_STYLE, Pattern.CASE_INSENSITIVE);
				Matcher m_style = p_style.matcher(inputString);
				inputString = m_style.replaceAll("");
				// 过滤html标签
				Pattern p_html = Pattern.compile(REGEX_HTML, Pattern.CASE_INSENSITIVE);
				Matcher m_html = p_html.matcher(inputString);
				inputString = m_html.replaceAll("");
				// // 过滤特殊字符
				// Pattern p_temp =
				// Pattern.compile(REGEX_TEMP,Pattern.CASE_INSENSITIVE);
				// Matcher m_temp = p_temp.matcher(inputString);
				// inputString = m_temp.replaceAll("");
				// 过滤两个以上空格
				Pattern p_space = Pattern.compile(REGEX_SPACE, Pattern.CASE_INSENSITIVE);
				Matcher m_space = p_space.matcher(inputString);
				inputString = m_space.replaceAll("");

				inputString.trim();
			} catch (Exception e) {
				System.err.println("Html2Text: " + e.getMessage());
			}
		}
		return inputString;
	}

	/**
	 * @param seperators
	 *            分隔符
	 * @param strOrigin
	 *            原始字符串
	 */
	public static String[] split(String seperators, String strOrigin) {
		return split(seperators, strOrigin, false);
	}

	/**
	 * 将字符串按指定分隔符分开
	 */
	public static String[] split(String seperators, String strOrigin, boolean include) {
		StringTokenizer tokens = new StringTokenizer(strOrigin, seperators, include);
		String[] result = new String[tokens.countTokens()];
		int i = 0;
		while (tokens.hasMoreTokens()) {
			result[i++] = tokens.nextToken();
		}
		return result;
	}

	/**
	 * @param strSrc
	 *            要进行替换操作的字符串
	 * @param strOld
	 *            要查找的字符串
	 * @param strNew
	 *            要替换的字符串
	 * @return 替换后的字符串
	 * 
	 *         <pre>
	 */
	public static final String replace(String strSrc, String strOld, String strNew) {
		if (strSrc == null || strOld == null || strNew == null)
			return "";
		int i = 0;
		if (strOld.equals(strNew)) // 避免新旧字符一样产生死循环
			return strSrc;
		if ((i = strSrc.indexOf(strOld, i)) >= 0) {
			char[] arr_cSrc = strSrc.toCharArray();
			char[] arr_cNew = strNew.toCharArray();
			int intOldLen = strOld.length();
			StringBuffer buf = new StringBuffer(arr_cSrc.length);
			buf.append(arr_cSrc, 0, i).append(arr_cNew);
			i += intOldLen;
			int j = i;
			while ((i = strSrc.indexOf(strOld, i)) > 0) {
				buf.append(arr_cSrc, j, i - j).append(arr_cNew);
				i += intOldLen;
				j = i;
			}
			buf.append(arr_cSrc, j, arr_cSrc.length - j);
			return buf.toString();
		}
		return strSrc;
	}

	/**
	 * @param inputString
	 *            原始字符串
	 * @param character
	 *            要查找的字符 计算指定字符出现的次数
	 */
	public static int count(String inputString, char character) {
		int n = 0;
		for (int i = 0; i < inputString.length(); i++) {
			if (inputString.charAt(i) == character)
				n++;
		}
		return n;
	}

	/**
	 * 初始化一个正则表达式
	 * 
	 * @param regexText正则表达
	 *            式
	 * */
	public static Pattern initPattern(String regexText) {
		if (!regexText.isEmpty()) {
			Pattern pattern = Pattern.compile(regexText, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);// 不区分大小写|多行匹配
			return pattern;
		}
		return null;
	}

	/**
	 * 
	 * @Title: isContainsChinese
	 * @Description: TODO(是否包含中文)
	 * @param @param str
	 * @param @return 设定文件
	 * @return boolean 返回类型
	 * @author lj
	 * @throws
	 */
	public static boolean isContainsChinese(String str) {
		boolean flg = false;
		try {
			Matcher matcher = pat.matcher(str);
			if (matcher.find())
				flg = true;
		} catch (Exception e) {
			return false;
		}
		return flg;
	}

	/**
	 * 判断输入的字符是否是IP地址的形式
	 * 
	 * @param strIp
	 *            源串
	 * @return boolean
	 */
	public static boolean isIp(String strIp) {
		boolean bRs = false;
		try {
			Matcher m_ip = p_ip.matcher(strIp);
			if (m_ip.find())
				bRs = true;
		} catch (Exception e) {
			return false;
		}
		return bRs;
	}

	/**
	 * 将数组中的每个元素两端加上给定的符号
	 * 
	 * @param aSource
	 *            源数组
	 * @param sChar
	 *            符号
	 * @return 处理后的字符串数组
	 */
	public String[] arrayAddSign(String[] aSource, String sChar) {
		String aReturn[] = new String[aSource.length];
		for (int i = 0; i < aSource.length; i++) {
			aReturn[i] = sChar + aSource[i] + sChar;
		}
		return aReturn;
	}

	/**
	 * 将数组中的元素连成一个以逗号分隔的字符串
	 * 
	 * @param aSource
	 *            源数组
	 * @return 字符串
	 */
	public String arrayToString(String[] aSource) {
		String sReturn = "";
		for (int i = 0; i < aSource.length; i++) {
			if (i > 0) {
				sReturn += ",";
			}
			sReturn += aSource[i];
		}
		return sReturn;
	}

	/**
	 * 将数组中的元素连成一个以逗号分隔的字符串
	 * 
	 * @param aSource
	 *            源数组
	 * @return 字符串
	 */
	public String arrayToString(int[] aSource) {
		String sReturn = "";
		for (int i = 0; i < aSource.length; i++) {
			if (i > 0) {
				sReturn += ",";
			}
			sReturn += aSource[i];
		}
		return sReturn;
	}

	/**
	 * 将数组中的元素连成一个以给定字符分隔的字符串
	 * 
	 * @param aSource
	 *            源数组
	 * @param sChar
	 *            分隔符
	 * @return 字符串
	 */
	public String arrayToString(String[] aSource, String sChar) {
		String sReturn = "";
		for (int i = 0; i < aSource.length; i++) {
			if (i > 0) {
				sReturn += sChar;
			}
			sReturn += aSource[i];
		}
		return sReturn;
	}

	/**
	 * 将两个字符串的所有元素连结为一个字符串数组
	 * 
	 * @param array1
	 *            源字符串数组1
	 * @param array2
	 *            源字符串数组2
	 * @return String[]
	 */
	public String[] arrayAppend(String[] array1, String[] array2) {
		int iLen = 0;
		String aReturn[] = null;
		if (array1 == null) {
			array1 = new String[0];
		}
		if (array2 == null) {
			array2 = new String[0];
		}
		iLen = array1.length;
		aReturn = new String[iLen + array2.length];
		// 将第一个字符串数组的元素加到结果数组中
		for (int i = 0; i < iLen; i++) {
			aReturn[i] = array1[i];
		}
		// 将第二个字符串数组的元素加到结果数组中
		for (int i = 0; i < array2.length; i++) {
			aReturn[iLen + i] = array2[i];
		}
		return aReturn;
	}

	/**
	 * 将两个对象数组中的所有元素连结为一个对象数组
	 * 
	 * @param array1
	 *            源字符串数组1
	 * @param array2
	 *            源字符串数组2
	 * @return Object[]
	 */
	public Object[] arrayAppend(Object[] array1, Object[] array2) {
		int iLen = 0;
		Object aReturn[] = null;
		if (array1 == null) {
			array1 = new Object[0];
		}
		if (array2 == null) {
			array2 = new Object[0];
		}
		iLen = array1.length;
		aReturn = new Object[iLen + array2.length];
		// 将第一个对象数组的元素加到结果数组中
		for (int i = 0; i < iLen; i++) {
			aReturn[i] = array1[i];
		}
		// 将第二个对象数组的元素加到结果数组中
		for (int i = 0; i < array2.length; i++) {
			aReturn[iLen + i] = array2[i];
		}
		return aReturn;
	}

	/**
	 * 拆分以逗号分隔的字符串,并存入String数组中
	 * 
	 * @param sSource
	 *            源字符串
	 * @return String[]
	 */
	public String[] strToArray(String sSource) {
		String aReturn[] = null;
		StringTokenizer st = null;
		st = new StringTokenizer(sSource, ",");
		aReturn = new String[st.countTokens()];
		int i = 0;
		while (st.hasMoreTokens()) {
			aReturn[i] = st.nextToken();
			i++;
		}
		return aReturn;
	}

	/**
	 * 拆分以给定分隔符分隔的字符串,并存入字符串数组中
	 * 
	 * @param sSource
	 *            源字符串
	 * @param sChar
	 *            分隔符
	 * @return String[]
	 */
	public static String[] strToArray(String sSource, String sChar) {
		String aReturn[] = null;
		StringTokenizer st = null;
		st = new StringTokenizer(sSource, sChar);
		int i = 0;
		aReturn = new String[st.countTokens()];
		while (st.hasMoreTokens()) {
			aReturn[i] = st.nextToken();
			i++;
		}
		return aReturn;
	}

	/**
	 * 拆分以给定分隔符分隔的字符串,并存入整型数组中
	 * 
	 * @param sSource
	 *            源字符串
	 * @param sChar
	 *            分隔符
	 * @return int[]
	 */
	public static int[] strToArray(String sSource, char sChar) {
		int aReturn[] = null;
		StringTokenizer st = null;
		st = new StringTokenizer(sSource, String.valueOf(sChar));
		int i = 0;
		aReturn = new int[st.countTokens()];
		while (st.hasMoreTokens()) {
			aReturn[i] = Integer.parseInt(st.nextToken());
			i++;
		}
		return aReturn;
	}

	/**
	 * 将以逗号分隔的字符串的每个元素加上单引号 如： 1000,1001,1002 --> '1000','1001','1002'
	 * 
	 * @param sSource
	 *            源串
	 * @return String
	 */
	public String addMark(String sSource) {
		String sReturn = "";
		StringTokenizer st = null;
		st = new StringTokenizer(sSource, ",");
		if (st.hasMoreTokens()) {
			sReturn += "'" + st.nextToken() + "'";
		}
		while (st.hasMoreTokens()) {
			sReturn += "," + "'" + st.nextToken() + "'";
		}
		return sReturn;
	}

	/**
	 * 对字符串进行md5加密
	 * 
	 * @param s
	 *            要加密的字符串
	 * @return md5加密后的字符串
	 */
	public static String MD5(String s) {
		char hexDigits[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };
		try {
			byte[] strTemp = s.getBytes();
			MessageDigest mdTemp = MessageDigest.getInstance("MD5");
			mdTemp.update(strTemp);
			byte[] md = mdTemp.digest();
			int j = md.length;
			char str[] = new char[j * 2];
			int k = 0;
			for (int i = 0; i < j; i++) {
				byte byte0 = md[i];
				str[k++] = hexDigits[byte0 >>> 4 & 0xf];
				str[k++] = hexDigits[byte0 & 0xf];
			}
			return new String(str);
		} catch (Exception e) {
			return null;
		}
	}

	/**
	 * 用于将字符串中的特殊字符转换成Web页中可以安全显示的字符串
	 * 可对表单数据据进行处理对一些页面特殊字符进行处理如'<','>','"',''','&'
	 * 
	 * @param strSrc
	 *            要进行替换操作的字符串
	 * @return 替换特殊字符后的字符串
	 * @since 1.0
	 */
	public static String htmlEncode(String strSrc) {
		if (strSrc == null)
			return "";
		char[] arr_cSrc = strSrc.toCharArray();
		StringBuffer buf = new StringBuffer(arr_cSrc.length);
		char ch;
		for (int i = 0; i < arr_cSrc.length; i++) {
			ch = arr_cSrc[i];
			if (ch == '<')
				buf.append("&lt;");
			else if (ch == '>')
				buf.append("&gt;");
			else if (ch == '"')
				buf.append("&quot;");
			else if (ch == '\'')
				buf.append("&#039;");
			else if (ch == '&')
				buf.append("&amp;");
			else
				buf.append(ch);
		}
		return buf.toString();
	}

	/**
	 * 用于将字符串中的特殊字符转换成Web页中可以安全显示的字符串
	 * 可对表单数据据进行处理对一些页面特殊字符进行处理如'<','>','"',''','&'
	 * 
	 * @param strSrc
	 *            要进行替换操作的字符串
	 * @param quotes
	 *            为0时单引号和双引号都替换，为1时不替换单引号，为2时不替换双引号，为3时单引号和双引号都不替换
	 * @return 替换特殊字符后的字符串
	 * @since 1.0
	 */
	public static String htmlEncode(String strSrc, int quotes) {
		if (strSrc == null)
			return "";
		if (quotes == 0) {
			return htmlEncode(strSrc);
		}
		char[] arr_cSrc = strSrc.toCharArray();
		StringBuffer buf = new StringBuffer(arr_cSrc.length);
		char ch;
		for (int i = 0; i < arr_cSrc.length; i++) {
			ch = arr_cSrc[i];
			if (ch == '<')
				buf.append("&lt;");
			else if (ch == '>')
				buf.append("&gt;");
			else if (ch == '"' && quotes == 1)
				buf.append("&quot;");
			else if (ch == '\'' && quotes == 2)
				buf.append("&#039;");
			else if (ch == '&')
				buf.append("&amp;");
			else
				buf.append(ch);
		}
		return buf.toString();
	}

	/**
	 * 和htmlEncode正好相反
	 * 
	 * @param strSrc
	 *            要进行转换的字符串
	 * @return 转换后的字符串
	 * @since 1.0
	 */
	public static String htmlDecode(String strSrc) {
		if (strSrc == null)
			return "";
		strSrc = strSrc.replaceAll("&lt;", "<");
		strSrc = strSrc.replaceAll("&gt;", ">");
		strSrc = strSrc.replaceAll("&quot;", "\"");
		strSrc = strSrc.replaceAll("&#039;", "'");
		strSrc = strSrc.replaceAll("&amp;", "&");
		return strSrc;
	}

	/**
	 * 把null值和""值转换成&nbsp; 主要应用于页面表格格的显示
	 * 
	 * @param str
	 *            要进行处理的字符串
	 * @return 转换后的字符串
	 */
	public static String str4Table(String str) {
		if (str == null)
			return "&nbsp;";
		else if (str.equals(""))
			return "&nbsp;";
		else
			return str;
	}

	/**
	 * 字符串从GBK编码转换为Unicode编码
	 * 
	 * @param text
	 * @return
	 */
	public static String StringToUnicode(String text) {
		String result = "";
		int input;
		StringReader isr;
		try {
			isr = new StringReader(new String(text.getBytes(), "GBK"));
		} catch (UnsupportedEncodingException e) {
			return "-1";
		}
		try {
			while ((input = isr.read()) != -1) {
				result = result + "&#x" + Integer.toHexString(input) + ";";
			}
		} catch (IOException e) {
			return "-2";
		}
		isr.close();
		return result;
	}

	/**
	 * @param inStr
	 * @return
	 */
	public static String gb2utf(String inStr) {
		char temChr;
		int ascInt;
		int i;
		String result = new String("");
		if (inStr == null) {
			inStr = "";
		}
		for (i = 0; i < inStr.length(); i++) {
			temChr = inStr.charAt(i);
			ascInt = temChr + 0;
			// System.out.println("1=="+ascInt);
			// System.out.println("1=="+Integer.toBinaryString(ascInt));
			if (Integer.toHexString(ascInt).length() > 2) {
				result = result + "&#x" + Integer.toHexString(ascInt) + ";";
			} else {
				result = result + temChr;
			}
		}
		return result;
	}

	/**
	 * @Title: trim() 增强版trim
	 * @Description: TODO(先将全角空格(如果有)转换成半角,然后去掉首尾空格)
	 * @param @param str
	 */
	public static String trim(String str) {
		return str.replace((char) SBC_SPACE, ' ').trim();
	}

	/**
	 * 
	 * @Title: normalizingSpace
	 * @Description: TODO(多个空格[全角,半角及另类的空白字符]标准处理为一个半角空格)
	 * @param @param str
	 */
	public static String normalizingSpace(String str) {
		Pattern p = null;
		Matcher m = null;
		if (null != str && !"".equals(str)) {
			p = Pattern.compile(REGEX_SPACE);
			m = p.matcher(str);
		}
		return m.replaceAll(" ");
	}

	/**
	 * 去除字符串中所包含的空格（包括:空格(全角，半角)、制表符、换页符等） 这里有一个神奇的" "字符
	 * 
	 * @param s
	 * @return
	 */
	public static String removeAllBlank(String str) {
		String result = "";
		if (null != str && !"".equals(str)) {
			result = str.replaceAll("[　*|　*| *| *| *|//s*]*", "");
		}
		return result;
	}

	/**
	 * <PRE>
	 * 半角字符->全角字符转换 
	 * 只处理空格，!到˜之间的字符，忽略其他
	 * </PRE>
	 */
	public static String bj2qj(String src) {
		if (src == null) {
			return src;
		}
		StringBuilder buf = new StringBuilder(src.length());
		char[] ca = src.toCharArray();
		for (int i = 0; i < ca.length; i++) {
			if (ca[i] == DBC_SPACE) { // 如果是半角空格，直接用全角空格替代
				buf.append(SBC_SPACE);
			} else if ((ca[i] >= DBC_CHAR_START) && (ca[i] <= DBC_CHAR_END)) { // 字符是!到~之间的可见字符
				buf.append((char) (ca[i] + CONVERT_STEP));
			} else { // 不对空格以及ascii表中其他可见字符之外的字符做任何处理
				buf.append(ca[i]);
			}
		}
		return buf.toString();
	}

	/**
	 * <PRE>
	 * 全角字符->半角字符转换 
	 * 只处理全角的空格，全角！到全角～之间的字符，忽略其他
	 * </PRE>
	 */
	public static String qj2bj(String src) {
		if (src == null) {
			return src;
		}
		StringBuilder buf = new StringBuilder(src.length());
		char[] ca = src.toCharArray();
		for (int i = 0; i < src.length(); i++) {
			if (ca[i] >= SBC_CHAR_START && ca[i] <= SBC_CHAR_END) { // 如果位于全角！到全角～区间内
				buf.append((char) (ca[i] - CONVERT_STEP));
			} else if (ca[i] == SBC_SPACE) { // 如果是全角空格
				buf.append(DBC_SPACE);
			} else { // 不处理全角空格，全角！到全角～区间外的字符
				buf.append(ca[i]);
			}
		}
		return buf.toString();
	}

	public static void main(String[] args) {
		String str = "2015年04月15日 19:38";
		System.out.println(normalizingSpace(str));

		System.out.println("|" + removeAllBlank(str) + "|");
		// String a = "121313/231//";
		// int length = a.length()-1;
		// int charAt = a.lastIndexOf("/");
		// System.out.println("length:"+length +"charAt:"+charAt);
		// isContainsChinese("http://www.ccgp-fujian.gov.cn/Article.cfm?id=352899&caidan=招标公告&caidan2=%E5%85%AC%E5%BC%80%E6%8B%9B%E6%A0%87&level=province&yqgg=0");
		// System.out
		// .println(SubStr("湖南卫视2012#青春跨年热血#钜制《隋唐英雄》今晚20:00恢宏献映。杨广篇首发：韬光养晦,只为一朝锋芒毕露;儒雅风流,怎知多情终被多情伤!@赵文瑄
		// 倾情演绎，末代君王隋炀帝的命运沉"));
		// System.out.println(replaceAll("  倾情演绎，末代君 王隋炀   "," ",""));
		// System.out.println(ConvertDate(new Date()).toString());
		// System.out.println(dateFormat("2013年02月18日14:43"));
		// String url
		// ="http://mn.sina.com.cn/travel/message/2013-01-22/100524882_8.html";
		// int next =getNextPageNum(url);
		// if(url.contains("_"))
		// {
		// String newu = url.replaceAll(next+"", next+1+"");
		// System.out.println(newu);
		// }
		// for(int i =1 ;i<=5;i++)
		// {
		// CreateNextUrl("http://ent.sina.com.cn/v/m/2013-01-25/14253844901.shtml",i);
		// }
		//
	}
}
