package com.duowan.hummingbird.db.aggr;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CollectMap extends BaseAggrFunction implements AggrFunction {

	@Override
	public Object exec(List groupBy, List<Object> values,Object[] params) {
		Map result = new HashMap();
		for(Object v : values) {
			result.putAll((Map)v);
		}
		return result;
	}

}
