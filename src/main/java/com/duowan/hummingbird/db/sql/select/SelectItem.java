package com.duowan.hummingbird.db.sql.select;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.mvel2.MVEL;
import org.springframework.util.Assert;

import com.duowan.hummingbird.db.aggr.AggrFunction;
import com.duowan.hummingbird.db.aggr.Avg;
import com.duowan.hummingbird.db.aggr.BloomFilterCountDistinct;
import com.duowan.hummingbird.db.aggr.Count;
import com.duowan.hummingbird.db.aggr.HyperLogLogCountDistinct;
import com.duowan.hummingbird.db.aggr.Max;
import com.duowan.hummingbird.db.aggr.Min;
import com.duowan.hummingbird.db.aggr.Sum;
import com.duowan.hummingbird.db.sql.select.SelectSql.GroupByValue;
import com.duowan.hummingbird.util.MVELUtil;

public class SelectItem {
	
	private static AggrFunctionRegister aggrFunctionRegister = AggrFunctionRegister.getInstance();
	
	private String alias; //别名

	private String func; //function
	private String[] params; //function parameters
	
	private String expr; // 代表选择单列
	
	boolean allTableColumns = false; // 代表选择所有*号
	

	public String getAlias() {
		return alias;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}

	public String getExpr() {
		return expr;
	}

	public void setExpr(String expr) {
		this.expr = expr;
	}

	public String[] getParams() {
		return params;
	}

	public void setParams(String[] params) {
		this.params = params;
	}

	public String getFunc() {
		return func;
	}

	public void setFunc(String func) {
		this.func = func;
	}
	
	public boolean isAllTableColumns() {
		return allTableColumns;
	}

	public void setAllTableColumns(boolean allTableColumns) {
		this.allTableColumns = allTableColumns;
	}

//	public Map execGroupBy(List groupBy, List<Map> rows) {
//		AggrFunction function = lookupFunction(this.func);
//
//		Map result = new HashMap(8);
//		String expr = getAggrExpr();
////		List<Object> values = extractValuesByMVEL(rows,params);
//		
//		List<Object> values = extractValues(rows, expr);
//		double value = function.exec(groupBy, values);
//		if (StringUtils.isBlank(alias)) {
//			result.put(this.func + "_" + expr, value);
//		} else {
//			result.put(alias, value);
//		}
//		return result;
//	}

	private String getAggrExpr() {
		Assert.notEmpty(params,"params must be not empty");
		String expr = params[0]; //first param is value
		Assert.hasText(expr, "expr must be not empty");
		return expr;
	}
	
	
	public Object execSelect(Map row) {
		if(allTableColumns) {
			return row;
		}
		
		return MVELUtil.eval(getExpr(),row); //TODO 需要性能优化
	}

	public static List<Object> extractValuesByMVEL(List<Map> rows,String[] columns) {
		Map map = new HashMap();
		map.put("rows", rows);
		String mvel = String.format("var result = [];\n foreach(row : rows) { result.add([%s]) }\n return result;",StringUtils.join(addPrefix("row.",columns),","));
//		System.out.println(mvel);
		Object list = MVEL.eval(mvel,map); //TODO 编译表达式,并且进行性能优化
		return (List)list;
	}
	
	public static List<String> addPrefix(String prefix,String[] columns) {
		List<String> r = new ArrayList<String>();
		for(String s : columns) {
			r.add(prefix + s);
		}
		return r;
	}

	private List<Object> extractValues(List<Map> list, String expr) {

		List<Object> result = new ArrayList();
		for (Map row : list) {
			Object v = MVELUtil.eval(expr, row);
			result.add(v);
		}
		return result;
	}

	public Map<GroupByValue,Object> execGroupBy(Map<GroupByValue, List<Map>> groupByedRows) {
		AggrFunction function = aggrFunctionRegister.getRequiredFunction(this.func);
		String aggrExpr = getAggrExpr();
		Object[] aggrAttachParams = getAttachAggrParamValues();
		Map<GroupByValue,List<Object>> funcParam = new HashMap<GroupByValue,List<Object>>();
		for(Map.Entry<GroupByValue, List<Map>> entry : groupByedRows.entrySet()) {
			List<Map> groupRows = entry.getValue();
			List<Object> values = extractValues(groupRows,aggrExpr);
			funcParam.put(entry.getKey(), values);
		}
		Map<GroupByValue,Object> aggrValue = function.execByBatch(funcParam,aggrAttachParams);
		return aggrValue;
	}

	private Object[] getAttachAggrParamValues() {
		String[] attachAggrParams = (String[])ArrayUtils.subarray(params, 1, params.length);
		Object[] attachAggrParamValues = (Object[])MVELUtil.eval("{"+StringUtils.join(attachAggrParams,",")+"}", new HashMap());
		return attachAggrParamValues;
	}

	public boolean isAggrFunction() {
		return aggrFunctionRegister.isAggrFunction(func);
	}

}