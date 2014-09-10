package com.duowan.hummingbird.db.aggr;

import java.util.List;

public class Sum extends BaseAggrFunction implements AggrFunction{

	@Override
	public Object exec(List groupBy, List<Object> values,Object[] params) {
		return sum(values);
	}

	public static double sum(List<Object> values) {
		double sum = 0;
		for(Object v : values) {
			if(v != null) {
				double num = toNumber(v);
				sum += num;
			}
		}
		return sum;
	}

	private static double toNumber(Object v) {
		if(v == null) {
			return 0;
		}
		double num = 0;
		if(v instanceof Number) {
			num = ((Number)v).doubleValue();
		}else if (v instanceof String) {
			num = Double.parseDouble((String)v);
		}else {
			num = Double.parseDouble((String)v.toString());
		}
		return num;
	}

}
