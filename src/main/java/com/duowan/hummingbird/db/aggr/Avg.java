package com.duowan.hummingbird.db.aggr;

import java.util.List;
import java.util.Map;

import com.duowan.hummingbird.util.MVELUtil;

public class Avg extends BaseAggrFunction implements AggrFunction{
	
	@Override
	public Object exec(List groupBy,List<Map> values,Object[] params){
		String expr = String.valueOf(params[0]);
		List<Object> querys = MVELUtil.extractValues(values, expr) ;
		
		return Sum.sum(querys) / Count.count(querys);
	}
	
}
