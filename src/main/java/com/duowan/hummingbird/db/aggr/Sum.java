package com.duowan.hummingbird.db.aggr;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.duowan.hummingbird.util.MVELUtil;

public class Sum extends BaseAggrFunction implements AggrFunction{
	private static Logger logger = LoggerFactory.getLogger(Sum.class);
	
	@Override
	public Object exec(List groupBy,List<Map> values,Object[] params){
		String expr = String.valueOf(params[0]);
		List<Object> querys = MVELUtil.extractNotNullValues(values, expr) ;
		
		return sum(querys);
	}

	public static double sum(List<Object> values) {
		if(CollectionUtils.isEmpty(values)) return 0;
		
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
		try {
			if(v == null) {
				return 0;
			}
			double num = 0;
			if(v instanceof Number) {
				num = ((Number)v).doubleValue();
			}else if (v instanceof String) {
				num = string2Number((String)v);
			}else {
				num = string2Number(v.toString());
			}
			return num;
		}catch(Exception e) {
			logger.error("Sum.toNumber error,v:"+v);
			return 0;
		}
	}

	private static double string2Number(Object v) {
		String strValue = (String)v;
		if(StringUtils.isBlank(strValue)) {
			return 0;
		}
		return  Double.parseDouble((String)v);
	}

}
