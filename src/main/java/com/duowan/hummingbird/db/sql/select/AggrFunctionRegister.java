package com.duowan.hummingbird.db.sql.select;

import java.util.HashMap;
import java.util.Map;

import com.duowan.hummingbird.db.aggr.AggrFunction;
import com.duowan.hummingbird.db.aggr.Avg;
import com.duowan.hummingbird.db.aggr.BloomFilterCountDistinct;
import com.duowan.hummingbird.db.aggr.CollectList;
import com.duowan.hummingbird.db.aggr.CollectMap;
import com.duowan.hummingbird.db.aggr.CollectSet;
import com.duowan.hummingbird.db.aggr.Count;
import com.duowan.hummingbird.db.aggr.CountDistinct;
import com.duowan.hummingbird.db.aggr.Max;
import com.duowan.hummingbird.db.aggr.Min;
import com.duowan.hummingbird.db.aggr.OrderFirstRow;
import com.duowan.hummingbird.db.aggr.Sum;
import com.duowan.hummingbird.db.aggr.bloomfilter.CountDistinctProvider;
import com.duowan.hummingbird.db.aggr.bloomfilter.HyperLogLogCountDistinct;

public class AggrFunctionRegister {

	private static AggrFunctionRegister instance = new AggrFunctionRegister();
	
	private CountDistinctProvider countDistinctProvider = null;

	public static AggrFunctionRegister getInstance() {
		return instance;
	}
	
	private  Map<String, AggrFunction> aggrFunctionMap = new HashMap();
	{
		aggrFunctionMap.put("order_first_row", new OrderFirstRow());
		aggrFunctionMap.put("cardinality_offer", new HyperLogLogCountDistinct());
		aggrFunctionMap.put("bf_count_distinct", new BloomFilterCountDistinct());
		aggrFunctionMap.put("count_distinct", new CountDistinct());
		aggrFunctionMap.put("count", new Count());
		aggrFunctionMap.put("sum", new Sum());
		aggrFunctionMap.put("min", new Min());
		aggrFunctionMap.put("max", new Max());
		aggrFunctionMap.put("avg", new Avg());
		aggrFunctionMap.put("collect_map", new CollectMap());
		aggrFunctionMap.put("collect_set", new CollectSet());
		aggrFunctionMap.put("collect_list", new CollectList());
	}
	
	
	private AggrFunctionRegister(){
	}
	
	public boolean isAggrFunction(String func) {
		return aggrFunctionMap.containsKey(func);
	}
	
	public AggrFunction getRequiredFunction(String funcName) {
		AggrFunction f = aggrFunctionMap.get(funcName);
		if (f == null) {
			throw new IllegalArgumentException("not found function by name:"
					+ funcName);
		}
		return f;
	}

	public void registerAggrFunction(String funcName, AggrFunction func) {
		aggrFunctionMap.put(funcName, func);
	}

	public CountDistinctProvider getCountDistinctProvider() {
		if(countDistinctProvider == null) {
			throw new RuntimeException("countDistinctProvider is null,not yet init");
		}
		return countDistinctProvider;
	}

	public void setCountDistinctProvider(CountDistinctProvider countDistinctProvider) {
		this.countDistinctProvider = countDistinctProvider;
	}
	
}
