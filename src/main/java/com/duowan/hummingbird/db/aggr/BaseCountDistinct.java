package com.duowan.hummingbird.db.aggr;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.duowan.hummingbird.db.sql.select.SelectSql.GroupByValue;

public abstract class BaseCountDistinct extends BaseAggrFunction implements AggrFunction{
	
	public Object exec(List groupBy,List<Object> values,Object[] params) {
		if(values == null || values.isEmpty()) {
			return 0;
		}
		
		Collection<Object> localDistinctedValues = (Collection<Object>) new HashSet(values);
		return distinctByHistory(groupBy,localDistinctedValues); 
	}
	
	//sum,count,avg,min,max,count_distinct
	public abstract int distinctByHistory(List groupBy,Collection localDistinctedValues);
	
}
