package com.duowan.hummingbird.db.aggr;

import java.util.List;
import java.util.Map;

public class CountDistinctProviderImpl implements CountDistinctProvider {

	private HyperLogLogClient hyperLogLogClient;
	private BloomFilterClientProvider bloomFilterClient;
	
	@Override
	public Map<String,Integer> bloomFilterNotContainsCountAndAdd(String bloomfilterDb,String bloomfilterGroup,List<DistinctRequest>  querys) {
		try {
			return bloomFilterClient.notContainsCountAndAdd(bloomfilterDb,bloomfilterGroup, querys);
		} catch (Exception e) {
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

	public void setBloomFilterClient(BloomFilterClientProvider bloomFilterClient) {
		this.bloomFilterClient = bloomFilterClient;
	}

	public void setHyperLogLogClient(HyperLogLogClient hyperLogLogClient) {
		this.hyperLogLogClient = hyperLogLogClient;
	}

	@Override
	public Map<String, List<DistinctData>> bloomFilterNotContainsCountAndAddAndReturnExt(String bloomfilterDb, String bloomfilterGroup, List<DistinctRequest> querys) {
		
		try {
			return bloomFilterClient.notContainsAndMarkBatchGroup(bloomfilterDb, bloomfilterGroup, querys);
		} catch (Exception e) {
			throw new RuntimeException("notContainsAndMarkBatchGroup() error",e);
		} 
	}
	
	

}
