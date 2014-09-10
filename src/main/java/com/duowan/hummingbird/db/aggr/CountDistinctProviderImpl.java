package com.duowan.hummingbird.db.aggr;

import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;

import com.duowan.realtime.computing.BloomFilterClient;
import com.duowan.realtime.computing.HyperLogLogPlusClient;
import com.duowan.realtime.thirft.api.BloomFilterException;
import com.duowan.realtime.thirft.api.BloomFilterGroupQuery;
import com.duowan.realtime.thirft.api.HyperLogLogPlusException;
import com.duowan.realtime.thirft.api.HyperLogLogPlusQuery;

public class CountDistinctProviderImpl implements CountDistinctProvider {

	private HyperLogLogPlusClient hyperLogLogPlusClient;
	private BloomFilterClient bloomFilterClient;
	
	@Override
	public Map<String,Integer> bloomFilterNotContainsCountAndAdd(String group,
			List<BloomFilterGroupQuery> querys) {
		try {
			return bloomFilterClient.notContainsCountAndAdd(group, querys);
		} catch (BloomFilterException e) {
			throw new RuntimeException("notContainsCountAndAdd() error",e);
		} catch (TException e) {
			throw new RuntimeException("notContainsCountAndAdd() error",e);
		}
	}

	@Override
	public Map<String,Integer> offerForCardinalityIncrement(String group,List<HyperLogLogPlusQuery> querys) {
		try {
			return hyperLogLogPlusClient.offerForCardinalityIncrement(group, querys);
		} catch (HyperLogLogPlusException e) {
			throw new RuntimeException("offerForCardinalityIncrement() error",e);
		} catch (TException e) {
			throw new RuntimeException("offerForCardinalityIncrement() error",e);
		}
	}

	public void setHyperLogLogPlusClient(HyperLogLogPlusClient hyperLogLogPlusClient) {
		this.hyperLogLogPlusClient = hyperLogLogPlusClient;
	}

	public void setBloomFilterClient(BloomFilterClient bloomFilterClient) {
		this.bloomFilterClient = bloomFilterClient;
	}

}
