package com.duowan.hummingbird.db.sql.select;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

public class FromItem {
	private String table;
	private String alias;
	private SelectSql subSelect;

	public FromItem(String table, String alias) {
		super();
		setTable(table);
		setAlias(alias);
	}

	public FromItem(SelectSql subSelectSql, String alias) {
		setSubSelect(subSelectSql);
		setAlias(alias);
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = StringUtils.trim(table);
	}

	public String getAlias() {
		return alias;
	}

	public void setAlias(String alias) {
		this.alias = StringUtils.trim(alias);
	}
	
	public List<Map> execute(Map<String,List<Map>> db,Map params) {
		if(StringUtils.isEmpty(table)) {
			return subSelect.execute(db,params);
		}else {
			return db.get(table);
		}
	}
	
	public SelectSql getSubSelect() {
		return subSelect;
	}

	public void setSubSelect(SelectSql subSelect) {
		this.subSelect = subSelect;
	}

	public String toString() {
		return table + " as " + alias;
	}
	
}
