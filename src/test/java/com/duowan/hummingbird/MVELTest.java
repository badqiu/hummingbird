package com.duowan.hummingbird;

import static junit.framework.Assert.assertTrue;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.mvel2.MVEL;

import com.duowan.common.redis.RedisTemplate;
import com.duowan.common.util.DateConvertUtils;
import com.duowan.hummingbird.util.MVELUtil;

public class MVELTest {

	@Test
	public void testSqlWhere() {
		Map map = new HashMap();
		map.put("game","ddt");
		map.put("game_server","s1");
		map.put("dur",100);
		System.out.println(MVEL.eval("return game == 'ddt' && dur > 10 && (game_server == 's1')",map));
		System.out.println(MVEL.eval("game == 'ddt' and dur > 10 ",map));
		System.out.println(MVEL.eval("game == 'ddt' and dur > 10 and game_server == 's1'",map));
	}
	
	@Test
	public void test() {
		System.out.println(Integer.MAX_VALUE);
		assertTrue(true);
		MVEL.eval("import junit.framework.Assert; Assert.assertTrue(true); System.out.println('123');",new HashMap());
		toMvelFunction(Math.class);
		toMvelFunction(DateConvertUtils.class);
		Map<String,Method> functions = toMvelFunction(StringUtils.class);
		System.out.println(functions);
		HashMap vars = new HashMap(functions);
		vars.put("tdate", new Date());
		System.out.println(MVEL.eval("System.out.println(extract(tdate,'yyyyMMdd'));return length('1000');",vars)+" " + vars);
	}
	
	static Map<String,Method> methods = new HashMap<String,Method>();
	public static Map<String,Method> toMvelFunction(Class clazz) {
		Map methods = new HashMap();
		StringBuilder sb = new StringBuilder();
		for(Method m : clazz.getMethods()) {
			if(methods.containsKey(m.getName())) {
				continue;
			}
			if(isReservedWord(m.getName())) {
				continue;
			}
			if(Modifier.isPublic(m.getModifiers()) && Modifier.isStatic(m.getModifiers())) {
				methods.put(m.getName(), m);
			}
		}
		
		return methods;
	}

	static Set reservedWords = new HashSet(Arrays.asList("contains"));
	private static boolean isReservedWord(String name) {
		return reservedWords.contains(name);
	}

	private static String toParamNames(Class<?>[] parameterTypes) {
		List array = new ArrayList();
		for(int i = 0; i < parameterTypes.length; i++) {
			array.add("param"+i);
		}
		return StringUtils.join(array,",");
	}
	
	@Test
	public void test2() {
		System.out.println(MVELUtil.eval("0== 0 or 1== 1", new HashMap()));
		System.out.println(MVELUtil.eval("0==0 || 1==0", new HashMap()));
		Map vars = new HashMap();
		vars.put("age", (double)20.1);
		vars.put("redis", new MockRedisTemplate());
		
		System.out.println(MVELUtil.eval("redis.incrBy('age',age);", vars));
		
	}
	
//	@Test
//	public void test2() {
//		
//		VariableResolverFactory myVarFactory = new MapVariableResolverFactory();
//		FunctionVariableResolverFactory functionRf = new FunctionVariableResolverFactory();
//		myVarFactory.setNextFactory(functionRf);
//		MVEL.eval("foo()", myVarFactory);
//	}
	
	@Test
	public void testSqlWhere2() throws ScriptException {
		ScriptEngineManager manager = new ScriptEngineManager();
	    for (ScriptEngineFactory factory : manager.getEngineFactories()) {
	        System.out.printf("language: %s, engine: %s%n", factory.getLanguageName(), factory.getEngineName());
	    }
	    ScriptEngine engine = manager.getEngineByName("SQL");
	    Object result = engine.eval("SELECT 1+2;");
	    System.out.println(result);
	}
	
	public static class MockRedisTemplate extends RedisTemplate {
		public Long incrBy(String key, long integer) {
			System.out.println("incrBy:"+key+" value:"+integer);
			return integer;
		}
	}
}
