package com.duowan.hummingbird.db.aggr;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.duowan.hummingbird.util.MVELUtil;

public class CollectMap extends BaseAggrFunction implements AggrFunction {

	@Override
	public Object exec(List groupBy,List<Map> values,Object[] params){
		String expr = String.valueOf(params[0]);
		List<Object> querys = MVELUtil.extractValues(values, expr) ;
		
		Map result = new HashMap();
		for(Object v : querys) {
			result.putAll((Map)v);
		}
		return result;
	}

}
