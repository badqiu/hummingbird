package com.duowan.hummingbird.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
	
	/**
	 * 存在性能问题
	 * @param leftAlias
	 * @param leftList
	 * @param rightAlias
	 * @param rightList
	 * @param joinCondition
	 * @return
	 */
	@Deprecated
	public static List<Map> innerJoin0(String leftAlias,Iterable leftList,String rightAlias,Iterable rightList,String joinCondition) {
		List<Map> resultList = new ArrayList<Map>();
		joinCondition = MVELUtil.sqlWhere2MVELExpression(joinCondition);
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
	
	public static List<Map> innerJoin(String leftAlias, Iterable leftList, String rightAlias, Iterable rightList, String joinCondition) {
		return join(leftAlias, leftList, rightAlias, rightList, joinCondition, true);
	}
	
	/**
	 * 
	 * @param alias1
	 * @param dataList1
	 * @param alias2
	 * @param dataList2
	 * @param joinCondition
	 * @param isInnerJoin
	 * @return
	 */
	private static List<Map> join(String alias1, Iterable dataList1, String alias2, Iterable dataList2, String joinCondition, boolean isInnerJoin) {
		Pattern p = Pattern.compile("(?i)(OR|[><()]{1})");
		Matcher m = p.matcher(joinCondition);
		boolean flag = false;
		if(m.find()) {
			flag = true;
			throw new RuntimeException("Join condition error! Only support \"AND\" and \"=\" condition, and do not support function operation : " + joinCondition);
		}
		if(flag) {
			return null;
		}
		
		List<String> key1List = new ArrayList<String>();
		List<String> key2List = new ArrayList<String>();
		buildJoinConditionKeyList(alias1, alias2, joinCondition, key1List, key2List);
		Map<String, List<Map>> dataList2Map = listToMap(dataList2, key2List);
		
		List<Map> resultList = new ArrayList<Map>();
		for(Object data1 : dataList1) {
			Map data1Map = Util.toMap(data1);
			if(data1Map == null) {
				continue;
			}
			String key = buildDataKey(key1List, data1Map);
			if(dataList2Map.containsKey(key)) {
				for(Map data2Map : dataList2Map.get(key)) {
					Map result = new HashMap();
					Util.putAll(result,data1Map);
					Util.putAll(result,data2Map);
					resultList.add(result);
				}
			}
			else {
				if(!isInnerJoin) {
					resultList.add(Util.newHashMap(data1Map));
				}
			}
		}
		return resultList;
	}

	/**
	 * @param dataList
	 * @param keyList
	 * @return
	 */
	private static Map<String, List<Map>> listToMap(Iterable dataList, List<String> keyList) {
		Map<String, List<Map>> dataListMap = new HashMap<String, List<Map>>();
		for(Object data : dataList) {
			Map dataMap = Util.toMap(data);
			String key = buildDataKey(keyList, dataMap);
			if(!dataListMap.containsKey(key)) {
				List<Map> list = new ArrayList<Map>();
				list.add(dataMap);
				dataListMap.put(key, list);
			}
			else {
				dataListMap.get(key).add(dataMap);
			}
		}
		return dataListMap;
	}

	/**
	 * @param keyList
	 * @param dataMap
	 * @return
	 */
	private static String buildDataKey(List<String> keyList, Map dataMap) {
		StringBuilder keyBuilder = new StringBuilder();
		for(String rightKey : keyList) {
			keyBuilder.append(dataMap.get(rightKey)).append("|");
		}
		String key = keyBuilder.toString();
		return key;
	}

	/**
	 * @param leftAlias
	 * @param rightAlias
	 * @param joinCondition
	 * @param leftKeyList
	 * @param rightKeyList
	 */
	private static void buildJoinConditionKeyList(String leftAlias, String rightAlias, String joinCondition, List<String> leftKeyList, List<String> rightKeyList) {
		Pattern p = Pattern.compile("(?i)(" + leftAlias + "|" + rightAlias + ")\\.(\\S+)\\s*=\\s*(" + leftAlias + "|" + rightAlias + ")\\.(\\S+)");
		Matcher m = p.matcher(joinCondition);
		while(m.find()) {
			if(StringUtils.equals(m.group(1), leftAlias)) {
				leftKeyList.add(m.group(2));
			}
			else if(StringUtils.equals(m.group(1), rightAlias)) {
				rightKeyList.add(m.group(2));
			}

			if(StringUtils.equals(m.group(3), leftAlias)) {
				leftKeyList.add(m.group(4));
			}
			else if(StringUtils.equals(m.group(3), rightAlias)) {
				rightKeyList.add(m.group(4));
			}
		}
	}
	
	/**
	 * 存在性能问题
	 * @param leftAlias
	 * @param leftList
	 * @param rightAlias
	 * @param rightList
	 * @param joinCondition
	 * @return
	 */
	@Deprecated
	public static List<Map> leftJoin0(String leftAlias,Iterable leftList,String rightAlias,Iterable rightList,String joinCondition) {
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
	
	public static List<Map> leftJoin(String leftAlias,Iterable leftList,String rightAlias,Iterable rightList,String joinCondition) {
		return join(leftAlias, leftList, rightAlias, rightList, joinCondition, false);
	}
	
	/**
	 * 存在性能问题
	 * @param leftAlias
	 * @param leftList
	 * @param rightAlias
	 * @param rightList
	 * @param joinCondition
	 * @return
	 */
	@Deprecated
	public static List<Map> rightJoin0(String leftAlias,Iterable leftList,String rightAlias,Iterable rightList,String joinCondition) {
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
	
	public static List<Map> rightJoin(String leftAlias,Iterable leftList,String rightAlias,Iterable rightList,String joinCondition) {
		return join(rightAlias, rightList, leftAlias, leftList, joinCondition, false);
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
