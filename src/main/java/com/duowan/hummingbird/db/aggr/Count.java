package com.duowan.hummingbird.db.aggr;

import java.util.List;

public class Count extends BaseAggrFunction implements AggrFunction{

	@Override
	public Object exec(List groupBy, List<Object> values,Object[] params) {
		return count(values);
	}

	public static int count(List<Object> values) {
		int count = 0;
		for(int i = 0; i < values.size(); i++) {
			if(values.get(i) != null) {
				count++;
			}
		}
		return count;
	}

}
