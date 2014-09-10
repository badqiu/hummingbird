package com.duowan.hummingbird.gamma.pool;

import java.util.HashMap;
import java.util.Map;

public class BloomFilterPool {
	static Map<String,BloomFilter> map = new HashMap();
	static BloomFilter bf = new BloomFilter(Integer.MAX_VALUE, Integer.MAX_VALUE); 
	public static BloomFilter getBloomFilter(String key) {
//		BloomFilter bf = map.get(key);
//		if(bf == null) {
//			bf = new BloomFilter(Integer.MAX_VALUE, Integer.MAX_VALUE);
//			map.put(key,bf);
//		}
//		return bf;
		return bf;
	}
}
