package com.duowan.hummingbird.util;

import java.util.ArrayList;
import java.util.List;

public class StringUtil {

	public static List<String> getNotNullStringValues(List<Object> values) {
		List list = new ArrayList<String>();
		for(Object item : values) {
			if(item != null) {
				list.add(item.toString());
			}
		}
		return list;
	}
	
}
