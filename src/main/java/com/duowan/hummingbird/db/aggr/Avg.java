package com.duowan.hummingbird.db.aggr;

import java.util.List;

public class Avg extends BaseAggrFunction implements AggrFunction{
	
	@Override
	public Object exec(List groupBy, List<Object> values,Object[] params) {
		return Sum.sum(values) / Count.count(values);
	}
	
}
