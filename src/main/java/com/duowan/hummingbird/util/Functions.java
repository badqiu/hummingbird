package com.duowan.hummingbird.util;

public class Functions {

	public static boolean cin(Object obj,Object... items) {
		for(Object item : items ) {
			if(item.equals(obj)) {
				return true;
			}
		}
		return false;
	}
	
}
