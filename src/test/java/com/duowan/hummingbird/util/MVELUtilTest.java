package com.duowan.hummingbird.util;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;

import org.junit.Test;

public class MVELUtilTest {

	@Test
	public void test() {
		String expr = MVELUtil.sqlWhere2MVELExpression("username = 123 and age != 100 or age >= 100 and height<= 200");
		
		assertEquals(expr,"username== 123 and age != 100 or age >= 100 and height<= 200");
	}

	@Test
	public void test2() {
		MVELUtil.eval("abc == 123",new HashMap());
	}
}
