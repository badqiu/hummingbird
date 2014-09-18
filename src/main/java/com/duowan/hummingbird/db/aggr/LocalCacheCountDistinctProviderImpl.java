package com.duowan.hummingbird.db.aggr;

import java.util.List;
import java.util.Map;

import com.duowan.realtime.thirft.api.BloomFilterGroupQuery;
import com.duowan.realtime.thirft.api.HyperLogLogPlusQuery;
import com.duowan.realtime.thirft.api.HyperLogLogQuery;

/**
 * 提供本地缓存的CountDistinctProvider
 * @author badqiu
 *
 */
public class LocalCacheCountDistinctProviderImpl implements CountDistinctProvider {

	private CountDistinctProvider proxy;
	
	public LocalCacheCountDistinctProviderImpl(CountDistinctProvider proxy) {
		super();
		this.proxy = proxy;
	}

	public Map<String, Integer> bloomFilterNotContainsCountAndAdd(String group,
			List<BloomFilterGroupQuery> querys) {
		return proxy.bloomFilterNotContainsCountAndAdd(group, querys);
	}

	public Map<String, Integer> offerForCardinality(String group,
			List<HyperLogLogQuery> query) {
		return proxy.offerForCardinality(group, query);
	}



}
