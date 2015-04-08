package com.duowan.hummingbird.db.aggr;

import java.util.List;
import java.util.Map;

import com.duowan.realtime.thirft.api.HyperLogLogQuery;
import com.yy.distinctservice.thirft.api.DistinctData;
import com.yy.distinctservice.thirft.api.DistinctRequest;

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

	public Map<String, Integer> bloomFilterNotContainsCountAndAdd(String bloomfilterDb,String bloomfilterGroup,List<DistinctRequest> querys) {
		return proxy.bloomFilterNotContainsCountAndAdd(bloomfilterDb,bloomfilterGroup, querys);
	}

	public void offer(String group,List<HyperLogLogQuery> query) {
		proxy.offer(group, query);
	}

	@Override
	public Map<String, List<DistinctData>> bloomFilterNotContainsCountAndAddAndReturnExt(String bloomfilterDb,String bloomfilterGroup, List<DistinctRequest> querys) {
		return proxy.bloomFilterNotContainsCountAndAddAndReturnExt(bloomfilterDb,bloomfilterGroup, querys);
	}

}
