package com.pic.ala;

import static com.pic.ala.util.LogUtil.toISO8601String;

/**
 *
 * @author gary
 * @since  2017年6月27日 上午10:15:46
 */
public class LogTimeTest {

	/**
	 * 將 Local DateTime 轉成 ISO 8601 Date Time
	 */
	public static void main(String[] args) {
		String localDateTimeString = "2017-02-11 21:16:53.999";
		System.out.println(toISO8601String(localDateTimeString));
		String iso8601DateTimeString = "2017-02-11T21:16:53.999+08:00";
		System.out.println(toISO8601String(iso8601DateTimeString));
	}
}
