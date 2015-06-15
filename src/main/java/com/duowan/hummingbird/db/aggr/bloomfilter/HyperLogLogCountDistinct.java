package com.duowan.hummingbird.db.aggr.bloomfilter;

import java.util.Collection;
import java.util.List;

import com.duowan.hummingbird.db.aggr.BaseCountDistinct;


public class HyperLogLogCountDistinct extends BaseCountDistinct{

	@Override
	public int distinctByHistory(List groupBy,
			Collection localDistinctedValues, Object[] params) {
//		return 0;
		throw new RuntimeException("not yet impl");
	}

//	@Override
//	public int distinctByHistory(List groupBy,Collection localDistinctedValues,Object[] params) {
////		return hyperLogLogByHistory(groupBy,localDistinctedValues);
//		throw new RuntimeException("not yet implement");
//	}
////
////	private int hyperLogLogByHistory(List groupBy,Collection<Object> localDistinctedColumns) {
////		String group = StringUtils.join(groupBy,"/");
////		HyperLogLog hll = HyperLogLogPool.getInstance(group);
////		return hll.offerForCardinalityIncrement(localDistinctedColumns);
////	}
////	
//	
//	@Override
//	public Map<GroupByValue, Object> execByBatch(Map<GroupByValue, List<Map>> map,Object[] params) {
//		if(ObjectUtils.isEmpty(params)) {
//			throw new RuntimeException("miss aggr params error,miss 'hllpGroup'");
//		}
//		String hllpGroup = (String)getAttachAggrParamValues(params)[0];
//		
//		Map<GroupByValue, List<Object>> funcParam = getSingleParam(map, params);
//		try {
//			List<HyperLogLogQuery> queryList = toHyperLogLogPlusQueryList(funcParam);
//			AggrFunctionRegister.getInstance().getCountDistinctProvider().offer(hllpGroup, queryList);
////			return BloomFilterCountDistinct.mapping2Result(map, resultMap);
//			return new HashMap();
//		}catch(Exception e) {
//			throw new RuntimeException("offerForCardinality exec error",e);
//		}
//	}
//
//	private List<HyperLogLogQuery> toHyperLogLogPlusQueryList(Map<GroupByValue, List<Object>> map) {
//		List<HyperLogLogQuery> result = new ArrayList<HyperLogLogQuery>(map.size());
//		for(Map.Entry<GroupByValue, List<Object>> entry : map.entrySet()) {
//			GroupByValue key = entry.getKey();
//			List<Object> values = entry.getValue();
//			String group = StringUtils.join(key.list,"/");
//			List<String> stringValues = new ArrayList(new HashSet(StringUtil.getNotNullStringValues(values)));
//			result.add(new HyperLogLogQuery(group,stringValues));
//		}
//		return result;
//	}

}
