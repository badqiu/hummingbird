package com.duowan.hummingbird.db.aggr;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.duowan.hummingbird.db.sql.select.SelectSql.GroupByValue;

public abstract class BaseAggrFunction implements AggrFunction{

	public BaseAggrFunction() {
		super();
	}

	public Map<GroupByValue, Object> execByBatch(Map<GroupByValue, List<Object>> funcParam,Object[] params) {
		Map<GroupByValue,Object> result = new HashMap();
		for(Map.Entry<GroupByValue, List<Object>> entry : funcParam.entrySet()) {
			GroupByValue key = entry.getKey();
			Object exec = exec(key.list,entry.getValue(),params);
			result.put(key, exec);
		}
		return result;
	}
	
	public void init(Map props){
	}

}