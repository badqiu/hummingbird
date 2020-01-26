package com.duowan.hummingbird.db.aggr;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.ComparatorUtils;

import com.duowan.hummingbird.util.MVELUtil;

public class Max extends BaseAggrFunction implements AggrFunction{

	@Override
	public Object exec(List groupBy,List<Map> values,Object[] params){
		String expr = String.valueOf(params[0]);
		List<Object> querys = MVELUtil.extractNotNullValues(values, expr) ;
		if(CollectionUtils.isEmpty(querys)) return null;
		
		
		Object max = Collections.max(querys,ComparatorUtils.NATURAL_COMPARATOR);
		return max;
	}

}
