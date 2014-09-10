package com.duowan.hummingbird;

import static junit.framework.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestUtil {

	public static void printRows(List<Map> select) {
		for(Map row : select) {
			System.out.println(row);
		}
	}

	public static void assertContains(List<Map> rows, String key, Object... values) {
		List<Object> keyValues = new ArrayList<Object>();
		for(Map row : rows) {
			keyValues.add(row.get(key));
		}
		
		for(Object v : values) {
			if(keyValues.contains(v)) {
				continue;
			}
			fail("not found value:"+v+" by key:"+key+", values:"+keyValues);
		}
	}
}
