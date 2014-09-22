package com.duowan.hummingbird.db.aggr;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.duowan.common.util.DateConvertUtils;
import com.duowan.hummingbird.db.sql.select.AggrFunctionRegister;
import com.duowan.hummingbird.db.sql.select.SelectSql.GroupByValue;
import com.duowan.hummingbird.util.StringUtil;
import com.duowan.hummingbird.util.bloomfilter.BloomFilter;
import com.duowan.hummingbird.util.bloomfilter.BloomFilterDB;
import com.duowan.realtime.thirft.api.BloomFilterGroupQuery;

public class BloomFilterCountDistinct extends BaseCountDistinct{

	private BloomFilterDB db = new BloomFilterDB("/data2/abc");
	
	public BloomFilterCountDistinct() {
	}
	
	public int distinctByHistory(List groupBy,Collection localDistinctedValues,Object[] params) {
		Assert.notEmpty(params,"bloomFilterName must be not empty");
		String bloomFilterName = (String)params[0];
		Assert.hasText(bloomFilterName,"bloomFilterName must be not empty");
		
		String group = StringUtils.join(groupBy,"/");
		
		Date groupDateValue = findDateValue(groupBy);
		String partition = "" + new DateConvertUtils().format(groupDateValue, "yyyyMMdd");
		
		BloomFilter bf = db.get(bloomFilterName, partition);
		return bf.notContainsCountAndAdd(group,localDistinctedValues);
	}

	private static Date findDateValue(List groupBy) {
		for(Object g : groupBy) {
			if(g instanceof Date) {
				return (Date)g;
			}
		}
		return null;
	}
	
	@Override
	public Map<GroupByValue, Object> execByBatch(Map<GroupByValue, List<Object>> map,Object[] params) {
		if(ObjectUtils.isEmpty(params)) {
			throw new RuntimeException("miss aggr params error,miss 'bloomfilterGroup'");
		}
		String bloomfilterGroup = (String)params[0];
		
		List<BloomFilterGroupQuery> bfGroupQuery = toBloomFilterGroupQueryList(map);
		Map<String,Integer> resultMap = AggrFunctionRegister.getInstance().getCountDistinctProvider().bloomFilterNotContainsCountAndAdd(bloomfilterGroup, bfGroupQuery );
		return mapping2Result(map, resultMap);
	}

	private static List<BloomFilterGroupQuery> toBloomFilterGroupQueryList(
			Map<GroupByValue, List<Object>> map) {
		List<BloomFilterGroupQuery> bfGroupQuery = new ArrayList<BloomFilterGroupQuery>(map.size());
		for(Map.Entry<GroupByValue, List<Object>> entry : map.entrySet()) {
			GroupByValue key = entry.getKey();
			List<Object> values = entry.getValue();
			String group = StringUtils.join(key.list,"/");
			Date groupDateValue = findDateValue(key.list);
			List<String> stringValues = new ArrayList(new HashSet(StringUtil.getNotNullStringValues(values)));
			bfGroupQuery.add(new BloomFilterGroupQuery(group,DateConvertUtils.format(groupDateValue, "yyyyMMdd"),stringValues));
		}
		return bfGroupQuery;
	}

	public static Map<GroupByValue, Object> mapping2Result(
			Map<GroupByValue, List<Object>> map, Map<String, Integer> resultMap) {
		Map<GroupByValue,Object> result = new HashMap<GroupByValue,Object>();
		for(Map.Entry<GroupByValue, List<Object>> entry : map.entrySet()) {
			GroupByValue key = entry.getKey();
			String group = StringUtils.join(key.list,"/");
			Object aggrResult = resultMap.get(group);
			if(aggrResult == null) {
				throw new RuntimeException("not found result for group:"+group);
			}
			result.put(key, aggrResult);
		}
		return result;
	}


	
}
