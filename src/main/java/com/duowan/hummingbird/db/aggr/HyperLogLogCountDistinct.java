package com.duowan.hummingbird.db.aggr;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.springframework.util.ObjectUtils;

import com.duowan.hummingbird.db.sql.select.AggrFunctionRegister;
import com.duowan.hummingbird.db.sql.select.SelectSql.GroupByValue;
import com.duowan.hummingbird.util.StringUtil;
import com.duowan.realtime.thirft.api.HyperLogLogPlusQuery;
import com.duowan.realtime.thirft.api.HyperLogLogQuery;

public class HyperLogLogCountDistinct extends BaseCountDistinct{

	@Override
	public int distinctByHistory(List groupBy,Collection localDistinctedValues,Object[] params) {
//		return hyperLogLogByHistory(groupBy,localDistinctedValues);
		throw new RuntimeException("not yet implement");
	}
//
//	private int hyperLogLogByHistory(List groupBy,Collection<Object> localDistinctedColumns) {
//		String group = StringUtils.join(groupBy,"/");
//		HyperLogLog hll = HyperLogLogPool.getInstance(group);
//		return hll.offerForCardinalityIncrement(localDistinctedColumns);
//	}
//	
	
	@Override
	public Map<GroupByValue, Object> execByBatch(Map<GroupByValue, List<Object>> map,Object[] params) {
		if(ObjectUtils.isEmpty(params)) {
			throw new RuntimeException("miss aggr params error,miss 'hllpGroup'");
		}
		String hllpGroup = (String)params[0];
		try {
			List<HyperLogLogQuery> queryList = toHyperLogLogPlusQueryList(map);
			Map<String,Integer> resultMap = AggrFunctionRegister.getInstance().getCountDistinctProvider().offerForCardinality(hllpGroup, queryList);
			return BloomFilterCountDistinct.mapping2Result(map, resultMap);
		}catch(Exception e) {
			throw new RuntimeException("offerForCardinality exec error",e);
		}
	}

	private List<HyperLogLogQuery> toHyperLogLogPlusQueryList(Map<GroupByValue, List<Object>> map) {
		List<HyperLogLogQuery> result = new ArrayList<HyperLogLogQuery>(map.size());
		for(Map.Entry<GroupByValue, List<Object>> entry : map.entrySet()) {
			GroupByValue key = entry.getKey();
			List<Object> values = entry.getValue();
			String group = StringUtils.join(key.list,"/");
			List<String> stringValues = new ArrayList(new HashSet(StringUtil.getNotNullStringValues(values)));
			result.add(new HyperLogLogQuery(group,stringValues));
		}
		return result;
	}

}