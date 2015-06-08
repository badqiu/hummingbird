package com.duowan.hummingbird.db.aggr.bloomfilter;

import java.util.List;
import java.util.Map;

import com.github.distinct_server.api.BloomFilterRequest;
import com.github.distinct_server.client.DistinctServiceClient;

public class CountDistinctProviderImpl implements CountDistinctProvider {

	private DistinctServiceClient distinctServiceClient;
	
	@Override
	public Map<String,Integer> bloomFilterNotContainsCountAndAdd(String bloomfilterVhost,String bloomfilterName,List<BloomFilterRequest>  querys) {
		try {
			return distinctServiceClient.batchBloomFilterNotContainsCountAndAdd(querys,bloomfilterVhost,bloomfilterName);
		} catch (Exception e) {
			throw new RuntimeException("notContainsCountAndAdd() error",e);
		} 
	}

	public void setDistinctServiceClient(DistinctServiceClient bloomFilterClient) {
		this.distinctServiceClient = bloomFilterClient;
	}


}
