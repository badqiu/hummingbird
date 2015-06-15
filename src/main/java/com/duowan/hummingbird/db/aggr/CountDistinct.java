package com.duowan.hummingbird.db.aggr;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.duowan.hummingbird.util.MVELUtil;

public class CountDistinct extends BaseAggrFunction implements AggrFunction{

	@Override
	public Object exec(List groupBy, List<Map> values, Object[] params) {
		String expr = String.valueOf(params[0]);
		List<Object> querys = MVELUtil.extractValues(values, expr) ;
		return countDistinct(querys);
	}

	public static int countDistinct(List<Object> querys) {
		return new HashSet(querys).size();
	}

}
