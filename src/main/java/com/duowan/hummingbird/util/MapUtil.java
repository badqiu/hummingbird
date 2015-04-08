package com.duowan.hummingbird.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;

public class MapUtil {

	/**
	 * 将所有key 转换为小写
	 * @param list
	 * @return
	 */
	public static List<Map<String, Object>> allMapKey2LowerCase(
			List<Map<String, Object>> list) {
		List<Map<String,Object>> result = new ArrayList(list.size());
		for(Map<String,Object> row : list) {
			Map newRow = new HashMap();
			for(Map.Entry<String, Object> entry : row.entrySet()) {
				newRow.put(StringUtils.lowerCase(entry.getKey()), entry.getValue());
			}
			result.add(newRow);
		}
		return result;
	}
	
	public static Map newMap(Object... args) {
		Map map = new HashMap();
		for(int i = 0; i < args.length; i+=2) {
			map.put(args[i], args[i+1]);
		}
		return map;
	}
	
	public static Map newLinkedMap(Object... args) {
		Map map = new LinkedHashMap();
		for(int i = 0; i < args.length; i+=2) {
			map.put(args[i], args[i+1]);
		}
		return map;
	}
	
	

	/**
	 * 解析一行数据，并返回Map数据, 数据格式示例: 
	 * <pre>
	 * key1=value1|key2=value2  =为mapKeysTerminatedChar |为collectionItemsTerminatedChar
	 * </pre>
	 * @param input
	 * @param fieldSeperator Map中不同key之间的分隔符
	 * @param mapKeySeperator Map中key value之间的分隔符
	 * @return
	 */
	public static Map<String,String> stringToMap (String input,char collectionItemsTerminatedChar,char mapKeysTerminatedChar) {
		Map<String,String> map = new HashMap<String, String>();
		if(StringUtils.isBlank(input)) {
			return Collections.emptyMap();
		}
		
		String[] array = StringUtils.split(input, collectionItemsTerminatedChar);
		for(String keyValue : array) {
			String[] pairs = StringUtils.split(keyValue,mapKeysTerminatedChar);
			if(pairs.length >= 2) {
				map.put(pairs[0], pairs[1]);
			}else if(pairs.length == 1) {
				map.put(pairs[0], null);
			}
		}
		return map ;
	}
	
	public static String mapToString(Map map,char collectionItemsTerminatedChar,char mapKeysTerminatedChar) {
		if(map == null) return null;
		
		StringBuilder sb = new StringBuilder();
		Set keySet = map.keySet();
		for(Iterator it = keySet.iterator();it.hasNext();) {
			Object key = it.next();
			Object value = map.get(key);
			
			sb.append(ObjectUtils.defaultIfNull(key,"")).append(mapKeysTerminatedChar).append(ObjectUtils.defaultIfNull(value,""));
			if(it.hasNext()) {
				sb.append(collectionItemsTerminatedChar);
			}
		}
		return sb.toString();
	}
}
