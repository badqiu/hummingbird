package com.duowan.hummingbird.db.sql.select;

import java.util.Comparator;
import java.util.Map;

public class OrderByComparator implements Comparator<Map> {

	private OrderBy[] orderBy;
	
	public OrderByComparator(OrderBy[] orderBy) {
		super();
		this.orderBy = orderBy;
	}

	@Override
	public int compare(Map o1, Map o2) {
		int compareResult = 0;
		for(OrderBy item : orderBy) {
			Object v1 = o1.get(item.getExpr());
			Object v2 = o2.get(item.getExpr());
			if(v1 instanceof Comparable && v2 instanceof Comparable) {
				compareResult = ((Comparable)v1).compareTo(v2);
				if(compareResult != 0) {
					return item.isAsc() ? compareResult :  -compareResult;
				}
			}
		}
		return 0;
	}
	
}
