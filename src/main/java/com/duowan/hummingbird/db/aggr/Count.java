package com.duowan.hummingbird.db.aggr;

import java.util.List;
import java.util.Map;

import com.duowan.hummingbird.util.MVELUtil;

public class Count extends BaseAggrFunction implements AggrFunction{

	@Override
	public Object exec(List groupBy,List<Map> values,Object[] params){
		String expr = String.valueOf(params[0]);
		List<Object> querys = MVELUtil.extractValues(values, expr) ;
		return count(querys);
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
