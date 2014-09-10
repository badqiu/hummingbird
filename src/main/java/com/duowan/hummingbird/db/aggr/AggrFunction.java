package com.duowan.hummingbird.db.aggr;

import java.util.List;
import java.util.Map;

import com.duowan.hummingbird.db.sql.select.SelectSql.GroupByValue;

/**
 * 聚集函数接口
 * 
 * @author badqiu
 *
 */
public interface AggrFunction {

	public void init(Map props);
	
	public Object exec(List groupBy,List<Object> values,Object[] params);

	public Map<GroupByValue, Object> execByBatch(Map<GroupByValue, List<Object>> map,Object[] params);

}
