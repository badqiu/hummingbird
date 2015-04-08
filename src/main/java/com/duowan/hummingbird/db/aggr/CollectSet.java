package com.duowan.hummingbird.db.aggr;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.duowan.hummingbird.util.MVELUtil;

public class CollectSet extends BaseAggrFunction implements AggrFunction {

	@Override
	public Object exec(List groupBy,List<Map> values,Object[] params){
		String expr = String.valueOf(params[0]);
		List<Object> querys = MVELUtil.extractValues(values, expr) ;
		Set set = new HashSet();
		addAll(querys, set);
		return set;
	}

	public static void addAll(List<Object> values, Collection target) {
		for(Object v : values) {
			if(v == null) {
				target.add(v);
			}else if(v instanceof Collection) {
				target.addAll((Collection)v);
			}else if(v.getClass().isArray()) {
				Object[] array = (Object[])v;
				Collections.addAll(target, array);
			}else {
				target.add(v);
			}
		}
	}



}
