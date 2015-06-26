package com.duowan.hummingbird.util;

import java.util.Date;

import com.github.rapid.common.util.DateConvertUtil;

public class Functions {

	/**
	 * 实现数据库类似 in('item1','item2') 语句功能
	 * @param obj
	 * @param items
	 * @return
	 */
	public static boolean cin(Object obj,Object... items) {
		for(Object item : items ) {
			if(item.equals(obj)) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * 对分钟进行取整操作
	 * @param date
	 * @param num
	 * @return
	 */
	public static Date roundMinute(Date date,int num) {
		int roundedMinute = date.getMinutes() / num * num;
		String dateString = DateConvertUtil.format(date, "yyyy-MM-dd HH:") + roundedMinute + ":00";
		return DateConvertUtil.parse(dateString, "yyyy-MM-dd HH:mm:ss");
	}
	
	
	public static Object ifnull(Object val ,Object defaultValue) {
		if(val == null) return defaultValue;
		return val;
	}
	
	public static Object IF(boolean val ,Object trueValue,Object falseValue) {
		if(val) 
			return trueValue;
		else
			return falseValue;
	}
	
}
