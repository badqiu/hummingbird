package com.duowan.hummingbird.db.aggr;

import java.util.Collections;
import java.util.List;

import org.apache.commons.collections.ComparatorUtils;

public class Max extends BaseAggrFunction implements AggrFunction{

	@Override
	public Object exec(List groupBy, List<Object> values,Object[] params) {
		Object max = Collections.max(values,ComparatorUtils.NATURAL_COMPARATOR);
		return max;
	}

}
