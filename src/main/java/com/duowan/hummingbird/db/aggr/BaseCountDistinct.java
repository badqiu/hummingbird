package com.duowan.hummingbird.db.aggr;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import com.duowan.hummingbird.db.sql.select.SelectSql.GroupByValue;
import com.duowan.hummingbird.util.MVELUtil;

public abstract class BaseCountDistinct extends BaseAggrFunction implements AggrFunction{
	
	public Object exec(List groupBy,List<Map> values,Object[] params) {
		if(values == null || values.isEmpty()) {
			return 0;
		}
		
		Collection<Object> localDistinctedValues = (Collection<Object>) new HashSet(values);
		return distinctByHistory(groupBy,localDistinctedValues,params); 
	}
	
	//sum,count,avg,min,max,count_distinct
	public abstract int distinctByHistory(List groupBy,Collection localDistinctedValues,Object[] params);
	
	// for v1 
	protected Map<GroupByValue, List<Object>> getSingleParam(Map<GroupByValue, List<Map>> map, Object[] params) {
		Map<GroupByValue,List<Object>> funcParam = new HashMap<GroupByValue,List<Object>>();
		for(Map.Entry<GroupByValue, List<Map>> entry : map.entrySet()) {
			List<Map> groupRows = entry.getValue();
			
			List<Object> values = MVELUtil.extractNotNullValues(groupRows,String.valueOf(params[0]));
			funcParam.put(entry.getKey(), values);
		}
		return funcParam;
	}

	protected Object[] getAttachAggrParamValues(Object[] params) {
		String[] attachAggrParams = (String[])ArrayUtils.subarray(params, 1, params.length);
		Object[] attachAggrParamValues = (Object[])MVELUtil.eval("{"+StringUtils.join(attachAggrParams,",")+"}", new HashMap());
		return attachAggrParamValues;
	}
}
