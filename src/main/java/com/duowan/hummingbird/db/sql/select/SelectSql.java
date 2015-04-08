package com.duowan.hummingbird.db.sql.select;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import net.sf.jsqlparser.JSQLParserException;

import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.lang.StringUtils;
import org.mvel2.MVEL;
import org.springframework.util.Assert;

import com.duowan.common.util.Profiler;
import com.duowan.hummingbird.db.sqlparser.SqlParser;
import com.duowan.hummingbird.db.sqlparser.SqlParserUtils;
import com.duowan.hummingbird.util.JoinUtil;
import com.duowan.hummingbird.util.MVELUtil;

public class SelectSql {
	private FromItem from;
	private String where;
	private String having;
	private OrderBy[] orderBy;
	private long limit;
	private long offset;
	private SelectItem[] selectItems;
//	private String partitionBy;
	private Join[] joins;
	
	private String groupBy;
	private String into; //into table
	
	private Properties props; // sql中的自定义属性,自创语法
	
	private transient GroupBy groupByObj;
	
	public FromItem getFrom() {
		return from;
	}

	public void setFrom(FromItem from) {
		this.from = from;
	}

	public String getWhere() {
		return where;
	}

	public void setWhere(String where) {
		this.where = where;
	}

	public String getGroupBy() {
		return groupBy;
	}

	public void setGroupBy(String groupBy) {
		this.groupBy = groupBy;
	}

	public String getHaving() {
		return having;
	}

	public void setHaving(String having) {
		this.having = having;
	}

	public OrderBy[] getOrderBy() {
		return orderBy;
	}

	public void setOrderBy(OrderBy[] orderBy) {
		this.orderBy = orderBy;
	}

	public long getLimit() {
		return limit;
	}

	public void setLimit(long limit) {
		this.limit = limit;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public Join[] getJoins() {
		return joins;
	}

	public void setJoins(Join[] joins) {
		this.joins = joins;
	}

	public SelectItem[] getSelectItems() {
		return selectItems;
	}

	public void setSelectItems(SelectItem[] selectItems) {
		this.selectItems = selectItems;
	}
	
	public String getInto() {
		return into;
	}

	public void setInto(String into) {
		this.into = into;
	}

	public List<Map> execute(Map<String,List<Map>> db, Map params) {
		return new SelectSqlExecutor(db,params).exec();
	}
	
	public class SelectSqlExecutor {
		private Map<String,List<Map>> db = null;
		private Map params = null;
		public SelectSqlExecutor(Map<String,List<Map>> db,Map params) {
			this.db = db;
			this.params = params;
		}
		
		public List<Map> exec() {
			List<Map> rows = from.execute(db,params);
			if(rows == null) {
				throw new RuntimeException("not found table by:"+from);
			}
			
			List<Map> joinRows = join(rows,joins,0);
			
			Profiler.enter("filterByWhere");
			List<Map> wheredRows = filterByWhere(joinRows,where);
			Profiler.release();
			
			List<Map> selectRows = groupByAndSelect(wheredRows);
			List<Map> havingedRows = having(selectRows);
			List<Map> orderByRows = orderBy(havingedRows);
			into(orderByRows);
			return limit(orderByRows);
		}
		
		private void into(List<Map> rows) {
			if(StringUtils.isEmpty(into)) {
				return;
			}
			//TODO 可能需要锁db,需要多线程测试
			List<Map> intoTable = db.get(into);
			if(intoTable == null) {
				intoTable = new ArrayList<Map>(rows);
				db.put(into, intoTable);
			}else {
				intoTable.addAll(rows);
			}
		}
		
		private List<Map> limit(List<Map> rows) {
			if(limit > 0 || offset > 0) {
				return rows.subList((int)offset, (int)(offset+limit));
			}else {
				return rows;
			}
		}
		
		private List<Map> orderBy(List<Map> rows) {
			if(orderBy == null) return rows;
			Collections.sort(rows,new OrderByComparator(orderBy));
			return rows;
		}
		
		private List<Map> having(List<Map> rows) {
			return filterByWhere(rows,having); 
		}
		
		private List<Map> groupByAndSelect(List<Map> wheredRows) {
			if(StringUtils.isEmpty(groupBy)) {
				return select(wheredRows);
			}else {
				Profiler.enter("groupBy");
				Map<GroupByValue,List<Map>> groupByedRows = groupBy(wheredRows);
				Profiler.release();
//				Profiler.enter("groupBySelect");
//				List<Map> selectRows = groupBySelect(groupByedRows);
//				Profiler.release();
//				return selectRows;
				try {
					Profiler.enter("selectForGroupByedMap");
					return selectForGroupByedMap(groupByedRows);
				}finally {
					Profiler.release();
				}
			}
		}
		
		private List<Map> select(List<Map> wheredRows) {
			List<Map> result = new ArrayList<Map>();
			for(Map row : wheredRows) {
				Map map = new HashMap();
				for(SelectItem item : selectItems) {
					Object value = item.execSelect(row);
					if(item.allTableColumns) {
						map.putAll((Map)row);
						continue;
					}

					
					if(StringUtils.isEmpty(item.getAlias())) {
						map.put(item.getExpr(),value);
					}else {
						map.put(item.getAlias(),value);
					}
				}
				
				result.add(map);
			}
			return result;
		}
		
		private List<Map> join(List<Map> rows, Join[] joins,int level) {
			if(joins == null || level >= joins.length) {
				return rows;
			}
			Join join = joins[level];
			List<Map> joinTable = join.getRightItem().execute(db,params);
			
			String leftAlias = StringUtils.defaultIfEmpty(from.getAlias(),from.getTable());
			String rightAlias = StringUtils.defaultIfEmpty(join.getRightItem().getAlias(),join.getRightItem().getTable());
			
			List<Map> joinedRows = JoinUtil.innerJoin(leftAlias,rows, rightAlias,joinTable, join.getOn());
			return join(joinedRows,joins,level+1);
		}
		
	}
	
	private List<Map> filterByWhere(List<Map> rows,String where) {
		if(StringUtils.isBlank(where)) {
			return rows;
		}
		String replacedWhere = MVELUtil.sqlWhere2MVELExpression(where) 
				;
		List<Map> result = new ArrayList<Map>();
		for(Map row : rows) {
			Boolean r = (Boolean)MVELUtil.eval(replacedWhere, row);//TODO 此处可以性能优化,避免需要多次 eval
			if(r) {
				result.add(row);
			}
		}
		return result;
		
//		String script = "var result = []; for(row : rows) {  if("+replacedWhere+") result.add(row); }; return result;";
//		Map vars = new HashMap(){
//			
//		};
//		vars.put("rows",rows);
//		List<Map> result = (List<Map>)MVELUtil.eval(script, vars);
//		return result;
	}
	


	private Map<GroupByValue,List<Map>> groupBy(List<Map> rows) {
		GroupBy groupByObj = getGroupByObj();
		Map<GroupByValue,List<Map>> result = new HashMap<GroupByValue,List<Map>>();
		int groupByItemSize = groupByObj.getGroupByList().size();
		Profiler.enter("groupBy loop");
		for(Map row : rows) {
			GroupByValue gbv = new GroupByValue(groupByItemSize);
			for(String group : groupByObj.getGroupByList()) {
				Object value = MVELUtil.eval(group, row); //此处可性能优化
				gbv.add(value);
			}
//			List groupByArrayValue = (List)MVEL.eval("["+groupBy+"]",row);
//			GroupByValue gbv = new GroupByValue(groupByArrayValue.toArray());
			
			List<Map> groupRows = result.get(gbv);
			if(groupRows == null) {
				groupRows = new ArrayList();
				result.put(gbv, groupRows);
			}
			groupRows.add(row);
		}
		Profiler.release();
		return result;
	}
	
//	private List<Map> groupBySelect(Map<GroupByValue, List> groupByedRows) {
//		List<Map> result = new ArrayList(groupByedRows.size());
//		
//		GroupBy groupBy = getGroupByObj();
//		for(Map.Entry<GroupByValue, List> entry : groupByedRows.entrySet()) {
//			Map itemGroupByResult = new HashMap();
//			GroupByValue gbv = entry.getKey();
//			List listValue = entry.getValue();
//			
//			Map groupByValueMap = toGruopByKeyMap(groupBy, gbv);
//			
//			for(SelectItem item : selectItems) {
//				if(groupByValueMap.containsKey(item.getExpr())) {
//					Object value = groupByValueMap.get(item.getExpr());
//					if(StringUtils.isBlank(item.getAlias())) {
//						itemGroupByResult.put(item.getExpr(), value);
//					}else {
//						itemGroupByResult.put(item.getAlias(), value);
//					}
//				}else {
//					Map itemValue = item.execGroupBy(gbv.list, listValue);
//					itemGroupByResult.putAll(itemValue);
//				}
//			}
//			
//			result.add(itemGroupByResult);
//		}
//		return result;
//	}

	private Map toGruopByKeyMap(GroupBy groupBy, GroupByValue gbv) {
		List<String> groupByList = groupBy.getGroupByList();
		Map groupByValueMap = new HashMap();//TODO 此项可节省以性能优化,通过groupBy Object提供的方法判断,避免产生groupByValueMap
		for(int i = 0; i < groupByList.size(); i++) {
			String group = groupByList.get(i);
			groupByValueMap.put(group,gbv.get(i));
		}
		return groupByValueMap;
	}
	
	private List<Map> selectForGroupByedMap(Map<GroupByValue, List<Map>> groupByedMap) {
		
		GroupBy groupBy = getGroupByObj();
		List<String> groupByList = groupBy.getGroupByList();
		
		Profiler.enter("execForAggrFunctionsResult");
		Map<SelectItem, Map> aggrFunctionsResult = execForAggrFunctionsResult(groupByedMap);
		Profiler.release();
		
		Profiler.enter("mapping to final aggr result");
		List<Map> result = new ArrayList(groupByedMap.size());
		for(Map.Entry<GroupByValue, List<Map>> entry : groupByedMap.entrySet()) {
			GroupByValue gbv = entry.getKey();
			Map groupByValueMap = toGruopByKeyMap(groupBy, gbv);
			Map itemGroupByResult = new HashMap();
			for(SelectItem item : selectItems) {
				if(item.isAggrFunction()) {
					Map<GroupByValue,Object> funcResult = aggrFunctionsResult.get(item);
					Object value = funcResult.get(gbv);
					String alias = StringUtils.defaultIfBlank(item.getAlias(), item.getFunc()+"_"+item.getParams()[0]);
					itemGroupByResult.put(alias,value);
				}else {
					Object value = MVELUtil.eval(item.getExpr(), groupByValueMap);
					String alias = StringUtils.defaultIfBlank(item.getAlias(), item.getExpr());
					itemGroupByResult.put(alias, value);
				}
			}
			result.add(itemGroupByResult);
		}
		Profiler.release();
		
		return result;
	}

	private Map<SelectItem, Map> execForAggrFunctionsResult(
			Map<GroupByValue, List<Map>> groupByedMap) {
		Map<SelectItem,Map> selectAggrMap = new HashMap<SelectItem,Map>();
		for(SelectItem item : selectItems) {
			if(item.isAggrFunction()) {
				Map<GroupByValue,Object> itemValue = item.execGroupBy(groupByedMap);
				selectAggrMap.put(item, itemValue);
			}else {
				if(this.groupBy != null && this.groupBy.contains(item.getExpr())) {
					continue;
				}
				Assert.isTrue(StringUtils.isEmpty(item.getFunc()),"not found such aggr function:"+item.getFunc()+"()");
			}
		}
		return selectAggrMap;
	}
	
	private GroupBy getGroupByObj() {
		if(groupByObj == null) {
			groupByObj = new GroupBy(groupBy);
		}
		return groupByObj;
	}
	
	public static class GroupByValue {
		public List list = null;
		public GroupByValue(int size) {
			list = new ArrayList(size);
		}
		public GroupByValue(Object[] v) {
			list = Arrays.asList(v);
		}
		public void add(Object value) {
			list.add(value);
		}
		public Object get(int index) {
			return list.get(index);
		}
		public String toString() {
			return String.valueOf(list);
		}
		@Override
		public int hashCode() {
//	        return Arrays.hashCode(list.toArray());
			return list.hashCode();
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			GroupByValue other = (GroupByValue) obj;
			if (list == null) {
				if (other.list != null)
					return false;
			} else if (!other.list.equals(other.list))
				return false;
			return true;
		}
	}
	
	private static class GroupBy {
		private String groupBy;
		private List<Serializable> mvelGroupByExprs;
		private List<String> groupByList;
		public GroupBy(String groupBy) {
			this.groupBy = groupBy;
			this.mvelGroupByExprs = toMVELExpression0();
			this.groupByList = parseGroupByList();
		}
		

		public List<Serializable> getMvelGroupByExprs() {
			return mvelGroupByExprs;
		}

		public List<String> getGroupByList() {
			return groupByList;
		}
		
		private List<Serializable> toMVELExpression0() {
			List<String> groupByList = parseGroupByList();
			List list = new ArrayList();
			for(String g : groupByList) {
				if(StringUtils.isNotBlank(g)) {
					list.add(MVEL.compileExpression(g));
				}
			}
			return list;
		}
		
		private List<String> parseGroupByList() {
			List<String> groupByList = SqlParserUtils.parseForGroupByItems(groupBy);
			return groupByList;
		}
	}
	
	private static Map<String,SelectSql> sqlCache = new LRUMap(20000);
	public static SelectSql parse(String querySql) {
		try {
			SelectSql result = sqlCache.get(querySql);
			if(result == null) {
				result = new SqlParser().parseForSelectSql(querySql);
				sqlCache.put(querySql, result);
			}
			return result;
		} catch (JSQLParserException e) {
			throw new RuntimeException("parse error on query:"+querySql,e);
		}
	}



}
