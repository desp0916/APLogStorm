package com.pic.ala.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

public class LogUtil {

	private static final DateTimeFormatter LOCAL_DATETIMEFORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(DateTimeZone.forID("Asia/Taipei"));
	private static final DateTimeFormatter ISO8601_DATETIMEFORMATTER = ISODateTimeFormat.dateTime().withZone(DateTimeZone.forID("Asia/Taipei"));

	/**
	 * Get current moment in ISO 8601 format
	 * http://stackoverflow.com/questions/3914404/how-to-get-current-moment-in-iso-8601-format
	 * 2016-01-20T10:53:52.457+08:00
	 * 2016-01-20T10:53:52.457+0800
	 * 2016-01-20T11:11:42.915Z
	 */
	public static String getISO8601Time() {
		return getISO8601Time("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
	}

	/**
	 * Get current moment in ISO 8601 format
	 * http://stackoverflow.com/questions/3914404/how-to-get-current-moment-in-iso-8601-format
	 * 2016-01-20T10:53:52.457+08:00
	 * 2016-01-20T10:53:52.457+0800
	 * 2016-01-20T11:11:42.915Z
	 */
	public static String getISO8601Time(String dateFormat) {
		// Choose one from the following two lines.
		DateFormat df = new SimpleDateFormat(dateFormat);
		// DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

		// Uncomment and modify the following line if you would like set the timezone explicitly.
		// df.setTimeZone(TimeZone.getTimeZone("Asia/Taipei"));
		return df.format(new Date());
	}

	/**
	 * 檢查某字串是否為數字？
	 *
	 * http://stackoverflow.com/questions/2563608/check-whether-a-string-is-parsable-into-long-without-try-catch
	 *
	 * @param str	要檢查的字串
	 * @return		檢查結果
	 */
	public static boolean isNumeric(String str) {
		if (str == null) {
			return false;
		}
		int sz = str.length();
		if (sz == 0) {
			return false;
		}
		for (int i = 0; i < sz; i++) {
			if (Character.isDigit(str.charAt(i)) == false) {
				return false;
			}
		}
		return true;
	}

	/**
	 * 檢查某字串是否為整數？
	 *
	 * http://stackoverflow.com/questions/237159/whats-the-best-way-to-check-to-see-if-a-string-represents-an-integer-in-java
	 *
	 * @param str	要檢查的字串
	 * @return		檢查結果
	 */
	public static boolean isInteger(String str) {
		if (str == null) {
			return false;
		}
		int length = str.length();
		if (length == 0) {
			return false;
		}
		int i = 0;
		if (str.charAt(0) == '-') {
			if (length == 1) {
				return false;
			}
			i = 1;
		}
		for (; i < length; i++) {
			char c = str.charAt(i);
			if (c < '0' || c > '9') {
				return false;
			}
		}
		return true;
	}



	/**
	 * 將字串解析並格式化成某特定日期時間格式
	 *
	 * @param dateTimeString		字串（應為日期時間）
	 * @param validFormats			可接受哪些日期時間格式
	 * @param outputFormat			希望輸出的日期時間格式
	 * @return						剖析結果
	 */
	public static String parseDateTime(String dateTimeString, String[] validFormats, String outputFormat) {
		for (int i = 0; i < validFormats.length; i++) {
			SimpleDateFormat format = new SimpleDateFormat(validFormats[i]);
			DateFormat df = new SimpleDateFormat(outputFormat);
			format.setLenient(false);
			try {
				Date temp = format.parse(dateTimeString);
				if (temp != null) {
					return df.format(temp);
				}
			} catch (ParseException e) {
			}
		}
		return null;
	}

	/**
	 * 檢查日期時間字串是否為「Local Date Time」格式（yyyy-MM-dd HH:mm:ss.SSS）？
	 *
	 * @param localDateTimeString	要檢查的日期時間字串
	 * @return						剖析結果
	 */
	public static Boolean isLocalDateTimeFormat(String dateTimeString) {
		if (isNullOrEmpty(dateTimeString)) {
			return false;
		}
		try {
			DateTime dt = LOCAL_DATETIMEFORMATTER.parseDateTime(dateTimeString);
			if (dt != null) {
				return true;
			}
		} catch (UnsupportedOperationException | IllegalArgumentException e) {
		}
		return false;
	}

	/**
	 * 檢查日期時間字串是否為「ISO8601 Date Time」格式（yyyy-MM-dd'T'HH:mm:ss.SSSZZ）？
	 *
	 * @param localDateTimeString	要檢查的日期時間字串
	 * @return						剖析結果
	 */
	public static Boolean isISO8601Format(String dateTimeString) {
		if (isNullOrEmpty(dateTimeString)) {
			return false;
		}
		try {
			DateTime dt = ISO8601_DATETIMEFORMATTER.parseDateTime(dateTimeString);
			if (dt != null) {
				return true;
			}
		} catch (UnsupportedOperationException | IllegalArgumentException e) {
		}
		return false;
	}

	/**
	 * 將字串轉換成 ISO8601 格式的字串（使用 Joda Time）
	 *
	 * @return
	 */
	public static String toISO8601String(String dateTimeString) {
		if (isISO8601Format(dateTimeString)) {
			return dateTimeString;
		}
		if (isLocalDateTimeFormat(dateTimeString)) {
			return localDateTimeToISO8601String(dateTimeString);
		}
		return null;
	}

	/**
	 * 將 Local Date Time 字串轉換為 ISO8601 格式的字串（使用 Joda Time）
	 *
	 * @param str					要轉換的字串
	 * @param dateTimeFormatter		日期時間格式轉換器
	 * @return						轉換結果字串
	 */
	public static String localDateTimeToISO8601String(String localDateTimeString) {
		DateTime dt = LOCAL_DATETIMEFORMATTER.parseDateTime(localDateTimeString);
		return dt.toString(ISO8601_DATETIMEFORMATTER);
	}

	/**
	 * 檢查字串是否為空或 Null？
	 *
	 * @param dateString		字串
	 * @return					檢查結果
	 */
	public static boolean isNullOrEmpty(String str) {
		if (str == null || ("").equals(str)) {
			return true;
		}
		return false;
	}

	/**
	 * 檢查日期字串是否合法？
	 * http://stackoverflow.com/questions/4528047/checking-the-validity-of-a-date
	 *
	 * @param dateString		日期字串
	 * @param validFormat		合法的日期格式
	 * @return					檢查結果
	 */
	public static boolean isDateValid(String dateString, String validFormat) {
		DateFormat df;
		df = (validFormat == null) ? new SimpleDateFormat("yyyy-MM-dd") : new SimpleDateFormat(validFormat);
		df.setLenient(false);
		try {
			df.parse(dateString);
			return true;
		} catch (ParseException e) {
			return false;
		}
	}

	private String cleanup(String str) {
		if (str != null) {
			return str.trim().replace("\n", "").replace("\t", "");
		} else {
			return str;
		}
	}
}
