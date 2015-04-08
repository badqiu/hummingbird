package com.duowan.hummingbird.db;

import java.util.List;
import java.util.Map;

import org.springframework.util.Assert;

/**
 * bird db的数据库连接对象
 * 
 * @author badqiu
 *
 */
public class BirdConnection {

	private boolean closed = false;
	
	private BirdDatabase currentDb;
	private MultiBirdDatabase multiBirdDatabase;
	
	public BirdConnection(MultiBirdDatabase multiBirdDatabase) {
		super();
		this.multiBirdDatabase = multiBirdDatabase;
	}

	public MultiBirdDatabase getMultiBirdDatabase() {
		return multiBirdDatabase;
	}

	public void setMultiBirdDatabase(MultiBirdDatabase multiBirdDatabase) {
		this.multiBirdDatabase = multiBirdDatabase;
	}

	public List<Map> select(String sql,Map params){
		assertOpen();
		if(isUseSql(sql)) {
			useDb(sql);
			return null;
		}else {
			Assert.notNull(getRequiredCurrentDb(),"currentDb must be not null");
			return getRequiredCurrentDb().select(sql, params);
		}
		
	}
	
	public int executeUpdate(String sql,Map params) throws Exception{
		assertOpen();
		if(isUseSql(sql)) {
			useDb(sql);
			return 0;
		}else {
			return getRequiredCurrentDb().executeUpdate(sql, params);
		}
	}
	
	public void insert(String table, List<Map> rows) throws Exception {
		getRequiredCurrentDb().insert(table, rows);
	}

	private BirdDatabase getRequiredCurrentDb() {
		Assert.notNull(currentDb,"currentDb must be not null");
		return currentDb;
	}

	public List<Map> truncate(String table) throws Exception {
		return getRequiredCurrentDb().truncate(table);
	}

	private void useDb(String sql) {
		String dbName = sql.substring(sql.indexOf("use ") + 4);
		currentDb = multiBirdDatabase.get(dbName);
		Assert.notNull(currentDb,"not found db by name:"+dbName);
	}

	private boolean isUseSql(String sql) {
		if(sql.toLowerCase().contains("use ")) {
			return true;
		}
		return false;
	}

	private void assertOpen() {
		if(closed) {
			throw new RuntimeException("error,connection closed");
		}
	}

	public void close() {
		closed = true;
	}
}
