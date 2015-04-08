package com.duowan.hummingbird.util;

import static org.junit.Assert.*;

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
		
		System.out.println(MVELUtil.eval(" 1==0 ? 'true' : 'false'",new HashMap()));
	}
	
	@Test
	public void test_in() {
		HashMap vars = new HashMap();
		vars.put("username", "badqiu");
		vars.put("age", 20);
		boolean result = (Boolean)MVELUtil.eval("exists(username,'blog','badqiu')",vars);
		assertTrue(result);
		
		result = (Boolean)MVELUtil.eval("exists(username,'blog','blog2')",vars);
		assertFalse(result);
	}
}
