package com.duowan.hummingbird.db.aggr;

import java.util.List;
import java.util.Map;

import com.duowan.realtime.thirft.api.BloomFilterGroupQuery;
import com.duowan.realtime.thirft.api.HyperLogLogPlusQuery;

public interface CountDistinctProvider {

	/**
	 * bloomfilter排重
	 * @param group
	 * @param values
	 * @return
	 */
	Map<String,Integer> bloomFilterNotContainsCountAndAdd(String group,List<BloomFilterGroupQuery> querys);
	
	/**
	 * Cardinality 排重
	 * @param group
	 * @param values
	 * @return
	 */
	Map<String,Integer> offerForCardinalityIncrement(String group,List<HyperLogLogPlusQuery> query);
	
}
