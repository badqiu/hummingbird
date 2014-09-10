package com.duowan.hummingbird.db.sql.select;

public class Join {
	private String on;
	private FromItem rightItem;
	private boolean innerJoin;
	private boolean leftJoin;
	private boolean rightJoin;
	
	public String getOn() {
		return on;
	}
	public void setOn(String on) {
		this.on = on;
	}
	public FromItem getRightItem() {
		return rightItem;
	}
	public void setRightItem(FromItem rightItem) {
		this.rightItem = rightItem;
	}
	
}
