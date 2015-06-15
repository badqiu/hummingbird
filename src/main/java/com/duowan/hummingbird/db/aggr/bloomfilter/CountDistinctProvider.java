package com.duowan.hummingbird.db.aggr.bloomfilter;

import java.util.List;
import java.util.Map;

import com.github.distinct_server.api.BloomFilterRequest;

public interface CountDistinctProvider {

	/**
	 * bloomfilter排重
	 * @param group
	 * @param values
	 * @return
	 */
	public Map<String,Integer> bloomFilterNotContainsCountAndAdd(String bloomfilterName,List<BloomFilterRequest> querys);

	
//	public Map<String, List<DistinctData>> bloomFilterNotContainsCountAndAddAndReturnExt(String bloomfilterDb,String bloomfilterGroup,List<DistinctRequest> querys);
	
	
//	/**
//	 * Cardinality 排重,得到当前的最新值
//	 * @param group
//	 * @param values
//	 * @return
//	 */
//	public void offer(String group,List<HyperLogLogQuery> query);
	
}
