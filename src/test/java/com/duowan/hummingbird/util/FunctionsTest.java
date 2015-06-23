package com.duowan.hummingbird.util;

import static org.junit.Assert.*;

import java.sql.Timestamp;
import java.util.Date;

import org.junit.Test;

import com.github.rapid.common.util.DateConvertUtil;

public class FunctionsTest {

	@Test
	public void testRoundMinute() {
		assertRoundMinute("1999-1-1 10:09:00","1999-01-01 10:05:00");
		assertRoundMinute("1999-1-1 10:01:00","1999-01-01 10:00:00");
		assertRoundMinute("1999-1-1 10:11:00","1999-01-01 10:10:00");
	}

	private void assertRoundMinute(String input,String output) {
		Date date = DateConvertUtil.parse("1999-1-1 10:09:00", "yyyy-MM-dd HH:mm:ss");
		Date result = Functions.roundMinute(date, 5);
		assertEquals(DateConvertUtil.format(result, "yyyy-MM-dd HH:mm:ss"),"1999-01-01 10:05:00");
	}

}
