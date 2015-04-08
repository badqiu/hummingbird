package com.duowan.hummingbird.db.aggr;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.duowan.hummingbird.util.MVELUtil;

public class CollectList extends BaseAggrFunction implements AggrFunction {


	@Override
	public Object exec(List groupBy,List<Map> values,Object[] params){
		String expr = String.valueOf(params[0]);
		List<Object> querys = MVELUtil.extractValues(values, expr) ;
		
		List list = new ArrayList();
		CollectSet.addAll(querys, list);
		return list;
	}

}
