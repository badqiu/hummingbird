package com.duowan.hummingbird.db.sql.select;

public class OrderBy {

	private String expr;
	private boolean asc;

	public OrderBy(String expr, boolean asc) {
		super();
		this.expr = expr;
		this.asc = asc;
	}

	public String getExpr() {
		return expr;
	}

	public void setExpr(String expr) {
		this.expr = expr;
	}

	public boolean isAsc() {
		return asc;
	}

	public void setAsc(boolean asc) {
		this.asc = asc;
	}

}
