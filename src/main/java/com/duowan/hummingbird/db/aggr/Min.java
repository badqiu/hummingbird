package com.duowan.hummingbird.db.aggr;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.ComparatorUtils;

import com.duowan.hummingbird.util.MVELUtil;

public class Min extends BaseAggrFunction implements AggrFunction{

	@Override
	public Object exec(List groupBy,List<Map> values,Object[] params){
		String expr = String.valueOf(params[0]);
		List<Object> querys = MVELUtil.extractNotNullValues(values, expr) ;
		
		if(querys.isEmpty()) return null;
		
		return Collections.min(querys,ComparatorUtils.NATURAL_COMPARATOR);
	}

}
