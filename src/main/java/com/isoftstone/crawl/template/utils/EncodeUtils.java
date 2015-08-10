package com.isoftstone.crawl.template.utils;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

public class EncodeUtils {
	private static final String DEFAULTCHARSET = "UTF-8";

	public static String convertEncoding(String str, String fromEncoding, String toEncoding) {
		String toStr = null;
		try {
			byte[] fromStr = str.getBytes(fromEncoding);
			toStr = new String(fromStr, toEncoding);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return toStr;
	}

	public static String convertEncoding(String str, String fromEncoding) {
		String toStr = null;
		try {
			toStr = new String(str.getBytes(fromEncoding));
			// String temp = new String(str.getBytes("UTF-8"),"ISO-8859-1");
			// System.out.println("str="+str);
			// toStr = new String(temp.getBytes("ISO-8859-1"),"UTF-8");
			// System.out.println("toStr="+toStr);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return toStr;
	}

	public static byte[] gbk2utf8(String chenese) {
		char c[] = chenese.toCharArray();
		byte[] fullByte = new byte[3 * c.length];
		for (int i = 0; i < c.length; i++) {
			int m = (int) c[i];
			String word = Integer.toBinaryString(m);
			// System.out.println(word);

			StringBuffer sb = new StringBuffer();
			int len = 16 - word.length();
			// 补零
			for (int j = 0; j < len; j++) {
				sb.append("0");
			}
			sb.append(word);
			sb.insert(0, "1110");
			sb.insert(8, "10");
			sb.insert(16, "10");

			// System.out.println(sb.toString());

			String s1 = sb.substring(0, 8);
			String s2 = sb.substring(8, 16);
			String s3 = sb.substring(16);

			byte b0 = Integer.valueOf(s1, 2).byteValue();
			byte b1 = Integer.valueOf(s2, 2).byteValue();
			byte b2 = Integer.valueOf(s3, 2).byteValue();
			byte[] bf = new byte[3];
			bf[0] = b0;
			fullByte[i * 3] = bf[0];
			bf[1] = b1;
			fullByte[i * 3 + 1] = bf[1];
			bf[2] = b2;
			fullByte[i * 3 + 2] = bf[2];

		}
		return fullByte;
	}

	public static byte[] getUTF8BytesFromGBKString(String gbkStr) {
		int n = gbkStr.length();
		byte[] utfBytes = new byte[3 * n];
		int k = 0;
		for (int i = 0; i < n; i++) {
			int m = gbkStr.charAt(i);
			if (m < 128 && m >= 0) {
				utfBytes[k++] = (byte) m;
				continue;
			}
			utfBytes[k++] = (byte) (0xe0 | (m >> 12));
			utfBytes[k++] = (byte) (0x80 | ((m >> 6) & 0x3f));
			utfBytes[k++] = (byte) (0x80 | (m & 0x3f));
		}
		if (k < utfBytes.length) {
			byte[] tmp = new byte[k];
			System.arraycopy(utfBytes, 0, tmp, 0, k);
			return tmp;
		}
		return utfBytes;
	}

	public static boolean isChinese(char c) {
		Character.UnicodeBlock ub = Character.UnicodeBlock.of(c);
		if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS || ub == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS || ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A || ub == Character.UnicodeBlock.GENERAL_PUNCTUATION || ub == Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION || ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS) {
			return true;
		}
		return false;
	}

	public static String formatUrl(String str, String charset) throws UnsupportedEncodingException {
		StringBuffer buf = null;
		if (!str.isEmpty() && str != null) {
			if (charset == null || charset.isEmpty()) {
				charset = DEFAULTCHARSET;
			}
			char[] ch = str.toCharArray();
			buf = new StringBuffer();
			for (int i = 0; i < ch.length; i++) {
				char c = ch[i];
				if (isChinese(c)) {
					buf.append(URLEncoder.encode(String.valueOf(c), charset));
				} else {
					buf.append(String.valueOf(c));
				}
			}
		}
		return buf.toString();
	}
}
