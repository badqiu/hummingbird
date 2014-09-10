package com.duowan.hummingbird.db;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import com.duowan.hummingbird.db.aggr.AggrFunction;
import com.duowan.hummingbird.db.aggr.CountDistinctProvider;
import com.duowan.hummingbird.db.sql.select.AggrFunctionRegister;
import com.duowan.hummingbird.db.sql.select.SelectSql;
import com.duowan.hummingbird.util.MVELUtil;

public class BirdDatabase {
	private Map<String,List<Map>> db = new ConcurrentHashMap<String, List<Map>>(); //数据库
	private Map<String,ReentrantLock> tableLock = new HashMap(); //数据库表锁
	private Properties props = new Properties(); //数据库属性
	
	
	public List<Map> getTable(String table) {
		return db.get(table);
	}
	
	public List<Map> select(String querySql)  {
		return select(querySql,new HashMap());
	}
	
	public List<Map> select(String querySql,Map params)  {
		SelectSql sql = SelectSql.parse(querySql);
		return sql.execute(db,params);
	}
	
	public int executeUpdate(String sql,Map params) throws Exception {
		throw new RuntimeException("not yet impl");
	}
	
	public void insert(String table,List<Map> rows) throws Exception {
		List<Map> tableData = db.get(table);
		if(tableData == null) {
			db.put(table, new ArrayList(rows));
		}else {
			tableData.addAll(rows);
		}
	}
	
	public List<Map> truncate(String table) throws Exception {
		try {
			return db.get(table);
		}finally {
			db.put(table, new ArrayList(0));
		}
	}
	
	public void delete(String sql,Map params) throws Exception {
		throw new RuntimeException("not yet implement");
	}
	
	public void lock(String table) {
		getLock(table).lock();
	}

	public void unlock(String table) {
		getLock(table).unlock();
	}
	
	private ReentrantLock getLock(String table) {
		ReentrantLock lock = tableLock.get(table);
		if(lock == null) {
			lock = new ReentrantLock();
			tableLock.put(table, lock);
		}
		return lock;
	}
	
	public static void registerFunctions(Map<String,Method> methods) {
		MVELUtil.registerFunctions(methods);
	}
	
	public static void registerAggrFunction(String funcName,AggrFunction func) {
		AggrFunctionRegister.getInstance().registerAggrFunction(funcName,func);
	}
	
	public static void setCountDistinctProvider(CountDistinctProvider countDistinctProvider) {
		AggrFunctionRegister.getInstance().setCountDistinctProvider(countDistinctProvider);
	}
}
