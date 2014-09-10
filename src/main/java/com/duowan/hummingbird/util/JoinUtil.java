package com.duowan.hummingbird.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.StringUtils;
import org.mvel2.MVEL;

/**
 * 根据joinCondition表达式，对两个List提供join功能
 * 
 * joinCondition表达式引擎为: MVEL,参考文档: <a href='http://mvel.codehaus.org/Getting+Started+for+2.0'>http://mvel.codehaus.org/Getting+Started+for+2.0</a>
 * 
 * <pre>
 * 示例使用:
 * # 左边的leftList固定的引用变量为left,右边rightList的固定引用变量为right
 * List list = JoinUtil.innerJoin(leftList, rightList, "left.game == right.game"); 
 * </pre>
 * @author badqiu
 *
 */
public class JoinUtil {
	
	public static List<Map> innerJoin(String leftAlias,Iterable leftList,String rightAlias,Iterable rightList,String joinCondition) {
		List<Map> resultList = new ArrayList<Map>();
		joinCondition = StringUtils.replace(joinCondition, "=", "==");
		for(Object left : leftList) {
			Map leftMap = Util.toMap(left);
			for(Object right : rightList) {
				Map rightMap = Util.toMap(right);
				Map result = new HashMap();
				if(Util.isTrue(leftAlias,leftMap,rightAlias,rightMap,joinCondition)) {
					Util.putAll(result,leftMap);
					Util.putAll(result,rightMap);
					resultList.add(result);
				}
			}
		}
		return resultList;
	}
	
	public static List<Map> leftJoin(String leftAlias,Iterable leftList,String rightAlias,Iterable rightList,String joinCondition) {
		List<Map> resultList = new ArrayList<Map>();
		for(Object left : leftList) {
			Map leftMap = Util.toMap(left);
			if(leftMap == null) {
				continue;
			}
			
			List joinList = new ArrayList();
			Map result = Util.newHashMap(leftMap);
			boolean join = false;
			for(Object right : rightList) {
				Map rightMap = Util.toMap(right);
				if(Util.isTrue(leftAlias,leftMap,rightAlias,rightMap,joinCondition)) {
					Util.putAll(result,rightMap);
					joinList.add(result);
					join = true;
				}
			}
			
			if(!join) {
				joinList.add(result);
			}
			
			resultList.addAll(joinList);
		}
		return resultList;
	}
	
	public static List<Map> rightJoin(String leftAlias,Iterable leftList,String rightAlias,Iterable rightList,String joinCondition) {
		List<Map> resultList = new ArrayList<Map>();
		for(Object right : rightList) {
			Map rightMap = Util.toMap(right);
			if(rightMap != null) {
				List joinList = new ArrayList();
				Map result = Util.newHashMap(rightMap);
				
				boolean join = false;
				for(Object left : leftList) {
					Map leftMap = Util.toMap(left);
					if(Util.isTrue(leftAlias,leftMap,rightAlias,rightMap,joinCondition)) {
						Util.putAll(result,leftMap);
						joinList.add(result);
						join = true;
					}
				}
				
				if(!join) {
					joinList.add(result);
				}
				
				resultList.addAll(joinList);
			}
		}
		return resultList;
	}
	
	static class Util {
		static HashMap newHashMap(Map map) {
			return map == null ? new HashMap() : new HashMap(map);
		}
		
		static void putAll(Map map,Map toPutAll) {
			if(toPutAll != null) {
				map.putAll(toPutAll);
			}
		}
		
		static boolean isTrue(String leftMapAlias,Map leftMap, String rightMapAlias,Map rightMap,
				String joinCondition) {
			if(leftMap == null) return false;
			if(rightMap == null) return false;
			
			Map model = new HashMap();
			model.put(leftMapAlias, leftMap);
			model.put(rightMapAlias, rightMap);
			Object obj = MVEL.eval(joinCondition,model);
			return ((Boolean)obj).booleanValue();
		}
		
		static Map toMap(Object obj)  {
			if(obj == null) return null;
			if(obj instanceof Map) return (Map)obj;
			try {
				return BeanUtils.describe(obj);
			}catch(Exception e) {
				throw new RuntimeException("BeanUtils.describe() error on obj:"+obj,e);
			}
		}
	}
}
