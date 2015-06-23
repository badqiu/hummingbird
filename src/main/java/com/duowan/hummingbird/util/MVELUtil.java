package com.duowan.hummingbird.util;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.mvel2.MVEL;
import org.mvel2.util.ParseTools;

import com.github.rapid.common.util.DateConvertUtil;

public class MVELUtil {
	private static boolean isReservedWord(String name) {
		return ParseTools.isReservedWord(name);
	}
	
	static Map<String,Serializable> exprCache = new HashMap();
	static Map<String,Object> methods = new HashMap<String,Object>();
	
	static {
		methods.putAll(getPublicStaticMethods(Math.class));
		methods.putAll(getPublicStaticMethods(DateConvertUtil.class));
		methods.putAll(getPublicStaticMethods(StringUtils.class));
		methods.putAll(getPublicStaticMethods(Functions.class));
		methods.putAll(getPublicStaticMethods(System.class));
		methods.putAll(getPublicStaticMethods(org.apache.commons.lang.math.NumberUtils.class));
	}
	
	public static Serializable getMVELCompileExpression(String expr) {
		Serializable result = exprCache.get(expr);
		if(result == null) {
			synchronized(exprCache) {
				result = MVEL.compileExpression(expr);
				exprCache.put(expr, result);
			}
		}
		return result;
	}
	
	public static Object eval(String expression,Map vars) {
		Object result = vars.get(expression);
		if(result == null) {
			result = MVEL.executeExpression(getMVELCompileExpression(expression),withMethodMap(vars));
		}
		return result;
	}
	
	public static Object executeExpression(Serializable compiledExpression,Map vars) {
		return MVEL.executeExpression(compiledExpression,withMethodMap(vars));
	}

	private static Map withMethodMap(Map vars) {
//		CachingMapVariableResolverFactory map = new CachingMapVariableResolverFactory(new HashMap(vars){
//			@Override
//			public boolean containsKey(Object key) {
//				return true;
//			}
//		});
//		map.setNextFactory(new CachingMapVariableResolverFactory(methods));
//		return map;
		Map result = new HashMap(){
			@Override
			public boolean containsKey(Object key) {
				return true;
			}
		};
		result.putAll(methods);
		result.putAll(vars);
		return result;
	}
	
	public static Map<String,Method> getPublicStaticMethods(Class clazz) {
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
	
	public static String sqlWhere2MVELExpression(String where) {
		return StringUtils.replace(StringUtils.replace(where.replaceAll("[^!><]=", "=="),"OR","||"),"AND","&&");
	}

	public static void registerFunctions(Map<String, Method> map) {
		methods.putAll(map);
	}

	public static <T> T executeExpression(Serializable expr, Map vars,
			Class<T> clazz) {
		return (T)MVEL.executeExpression(expr,withMethodMap(vars),clazz);
	}

	public static List<Object> extractNotNullValues(List<Map> list, String expr) {
		return extractValues(list, expr, true);
	}
	
	public static List<Object> extractValues(List<Map> list, String expr) {
		return extractValues(list, expr, false);
	}
	
	public static List<Object> extractValues(List<Map> list, String expr,boolean ignoreNullValue) {
		List<Object> result = new ArrayList<Object>();
		for (Map row : list) {
			Object v = MVELUtil.eval(expr, row);
			if(ignoreNullValue && v == null){// null 值不参与做聚合
				continue ;
			}
			result.add(v);
		}
		return result;
	}
	
}
