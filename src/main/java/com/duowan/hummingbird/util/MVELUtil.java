package com.duowan.hummingbird.util;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.mvel2.MVEL;
import org.mvel2.integration.VariableResolverFactory;
import org.mvel2.integration.impl.CachingMapVariableResolverFactory;
import org.mvel2.util.ParseTools;

import com.duowan.common.util.DateConvertUtils;

public class MVELUtil {
	private static boolean isReservedWord(String name) {
		return ParseTools.isReservedWord(name);
	}
	
	static Map<String,Serializable> exprCache = new HashMap();
	static Map<String,Method> methods = new HashMap<String,Method>();
	
	static {
		methods.putAll(getPublicStaticMethods(Math.class));
		methods.putAll(getPublicStaticMethods(DateConvertUtils.class));
		methods.putAll(getPublicStaticMethods(StringUtils.class));
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

	private static VariableResolverFactory withMethodMap(Map vars) {
		CachingMapVariableResolverFactory map = new CachingMapVariableResolverFactory(vars);
		map.setNextFactory(new CachingMapVariableResolverFactory(methods));
		return map;
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
		return StringUtils.replace(StringUtils.replace(where.replaceAll("[^!><]=", "=="),"OR","or"),"AND","and");
	}

	public static void registerFunctions(Map<String, Method> map) {
		methods.putAll(map);
	}

}
