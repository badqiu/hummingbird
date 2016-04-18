package com.duowan.hummingbird.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;

import org.junit.Test;

public class MVELUtilTest {

	@Test
	public void test() {
		String expr = MVELUtil.sqlWhere2MVELExpression("username = 123 and age != 100 or age >= 100 and height<= 200");
		
		assertEquals(expr,"username== 123 and age != 100 or age >= 100 and height<= 200");
	}

	@Test
	public void test_to_double() {
		HashMap vars = new HashMap();
		vars.put("m1", 100);
		vars.put("m2", 200);
		Object result = MVELUtil.eval("toDouble(null) - toDouble(100)",vars);
		System.out.println(result);
		
		result = MVELUtil.eval("toDouble(m1) - toDouble(not_exist_var)",vars);
		System.out.println(result);
		
		result = MVELUtil.eval("toDouble(m1) - toDouble(m2)",vars);
		System.out.println(result);
		
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
