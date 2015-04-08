package com.duowan.hummingbird.db;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MultiBirdDatabase {

	private Map<String, BirdDatabase> schemas = new HashMap<String, BirdDatabase>();

	public Map<String, BirdDatabase> getSchemas() {
		return schemas;
	}

	public void newDatabase(String schema) {
		schemas.put(schema, new BirdDatabase());
	}
	
	public BirdDatabase get(String schema) {
		return schemas.get(schema);
	}

	public List<Map> getTable(String schema,String table) {
		return get(schema).getTable(table);
	}

	public List<Map> select(String schema,String querySql) {
		return get(schema).select(querySql);
	}

	public List<Map> select(String schema,String querySql, Map params) {
		return get(schema).select(querySql, params);
	}

	public int executeUpdate(String schema,String sql, Map params) throws Exception {
		if(get(schema) == null) {
			newDatabase(schema);
		}
		return get(schema).executeUpdate(sql, params);
	}

	public void insert(String schema,String table, List<Map> rows) throws Exception {
		if(get(schema) == null) {
			newDatabase(schema);
		}
		get(schema).insert(table, rows);
	}

	public List<Map> truncate(String schema,String table) throws Exception {
		return get(schema).truncate(table);
	}

	public void delete(String schema,String sql, Map params) throws Exception {
		get(schema).delete(sql, params);
	}

	public void lock(String schema,String table) {
		get(schema).lock(table);
	}

	public void unlock(String schema,String table) {
		get(schema).unlock(table);
	}

	
	public BirdConnection newConnection() {
		BirdConnection result = new BirdConnection(this);
		return result;
	}
}
