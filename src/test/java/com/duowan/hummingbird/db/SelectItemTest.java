package com.duowan.hummingbird.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.duowan.hummingbird.TestData;
import com.duowan.hummingbird.db.sql.select.SelectItem;

public class SelectItemTest {

	@Test
	public void test() {
		List<Map> rows = new ArrayList(TestData.getTestDatasList(2000));
		printRowsIfNull(rows,"passport");
		rows.add(new HashMap());
		List<Object> result = SelectItem.extractValuesByMVEL(rows, new String[]{"passport","game"});
		System.out.println(result);
		
		result = SelectItem.extractValuesByMVEL(rows, new String[]{"dur"});
		System.out.println(result);
	}

	private void printRowsIfNull(List<Map> rows, String key) {
		for(Map row : rows) {
			if(row.get(key) == null) {
				System.out.println("is null on "+key+" => "+row);
			}
		}
	}

}
