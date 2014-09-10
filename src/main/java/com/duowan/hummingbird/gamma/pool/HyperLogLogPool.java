package com.duowan.hummingbird.gamma.pool;

import java.util.HashMap;
import java.util.Map;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;

public class HyperLogLogPool {

	static Map<String,HyperLogLog> map = new HashMap<String,HyperLogLog>();
	public static HyperLogLog getInstance(String key) {
		HyperLogLog hll = map.get(key);
		if(hll == null) {
			hll = new HyperLogLog(25);
			map.put(key, hll);
		}
		return hll;
	}
	
}
