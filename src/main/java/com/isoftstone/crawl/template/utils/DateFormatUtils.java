package com.isoftstone.crawl.template.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 
 * @ClassName: DateFormatter
 * @Description: TODO(日期格式化)
 * @author lj
 * @date 2014年7月25日 下午12:42:16
 * 
 */
public class DateFormatUtils {
	public static String nDaysBeforeToday(int n) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Calendar rightNow = Calendar.getInstance();

		rightNow.add(Calendar.DAY_OF_MONTH, -n);

		return sdf.format(rightNow.getTime());
	}

	public static Date nDaysBeforeTodayDate(int n) {
		Calendar rightNow = Calendar.getInstance();

		rightNow.add(Calendar.DAY_OF_MONTH, -n);

		return rightNow.getTime();
	}

	public static Date nSecondBeforeNow(int n) {
		Calendar rightNow = Calendar.getInstance();
		rightNow.add(Calendar.SECOND, -n);
		return rightNow.getTime();
	}

	public static Date nMinuteBeforeNow(int n) {
		Calendar rightNow = Calendar.getInstance();
		rightNow.add(Calendar.MINUTE, -n);
		return rightNow.getTime();
	}

	public static Date nHourBeforeNow(int n) {
		Calendar rightNow = Calendar.getInstance();
		rightNow.add(Calendar.HOUR_OF_DAY, -n);
		return rightNow.getTime();
	}

	public static Date nHourBefore(Date s,int n) {
		Calendar rightNow = Calendar.getInstance();
		rightNow.setTime(s);
		rightNow.add(Calendar.HOUR_OF_DAY, -n);
		return rightNow.getTime();
	}
	public static void main(String[] args) {
		
	}
}
