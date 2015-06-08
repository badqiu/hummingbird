package com.duowan.hummingbird.db.aggr.bloomfilter;

import java.util.List;
import java.util.Map;

import com.github.distinct_server.api.BloomFilterRequest;

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

	@Override
	public Map<String, Integer> bloomFilterNotContainsCountAndAdd(
			String bloomfilterVhost, String bloomfilterName,
			List<BloomFilterRequest> querys) {
		return null;
	}


}
