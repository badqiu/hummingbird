package com.duowan.hummingbird.db.aggr;

import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;

import com.duowan.realtime.computing.BloomFilterClient;
import com.duowan.realtime.computing.HyperLogLogClient;
import com.duowan.realtime.thirft.api.BloomFilterException;
import com.duowan.realtime.thirft.api.BloomFilterGroupQuery;
import com.duowan.realtime.thirft.api.HyperLogLogPlusException;
import com.duowan.realtime.thirft.api.HyperLogLogQuery;

public class CountDistinctProviderImpl implements CountDistinctProvider {

	private HyperLogLogClient hyperLogLogClient;
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
	public void offer(String group,List<HyperLogLogQuery> querys) {
		try {
			hyperLogLogClient.offer(group, querys);
		} catch (HyperLogLogPlusException e) {
			throw new RuntimeException("offerForCardinalityIncrement() error",e);
		} catch (TException e) {
			throw new RuntimeException("offerForCardinalityIncrement() error",e);
		}
	}

	public void setBloomFilterClient(BloomFilterClient bloomFilterClient) {
		this.bloomFilterClient = bloomFilterClient;
	}

	public void setHyperLogLogClient(HyperLogLogClient hyperLogLogClient) {
		this.hyperLogLogClient = hyperLogLogClient;
	}
	
	

}
