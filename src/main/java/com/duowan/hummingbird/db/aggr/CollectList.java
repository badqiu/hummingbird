package com.duowan.hummingbird.db.aggr;

import java.util.ArrayList;
import java.util.List;

public class CollectList extends BaseAggrFunction implements AggrFunction {


	@Override
	public Object exec(List groupBy, List<Object> values,Object[] params) {
		List list = new ArrayList();
		CollectSet.addAll(values, list);
		return list;
	}

}
