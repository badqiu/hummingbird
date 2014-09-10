package com.duowan.hummingbird.db.aggr;

import java.util.Collections;
import java.util.List;

import org.apache.commons.collections.ComparatorUtils;

public class Min extends BaseAggrFunction implements AggrFunction{

	@Override
	public Object exec(List groupBy, List<Object> values,Object[] params) {
		return Collections.min(values,ComparatorUtils.NATURAL_COMPARATOR);
	}

}
