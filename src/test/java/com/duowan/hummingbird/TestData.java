package com.duowan.hummingbird;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.lang.time.DateUtils;

import com.github.rapid.common.util.DateConvertUtil;

public class TestData {

	public static  Map[] getTestDatas(int count,String... keyValuePairs) {
		List<Map> rows = new ArrayList();
		
		Date dt = DateConvertUtil.parse("1999-1-1", "yyyy-MM-dd");
		for(int i = 0; i < count; i++) {
			Map row = new HashMap();
			row.put("id", i);
			row.put("game", data(i,"ddt","as","hz"));
			row.put("game_server", data(i,"s1","s2","s3","s4","s5","s6"));
			row.put("channel", data(i,"channel1","channel2"));
			row.put("ref", data(i,"ref1"));
			row.put("dt", new Timestamp(DateUtils.addDays(dt, i % 4).getTime()));
			row.put("stime", new Timestamp(DateUtils.addDays(dt, i % 4).getTime()));
			row.put("passport", i % 100);
			row.put("dur", i % 1000);
			row.put("keynull", null);
			row.put("num", i);
			row.put("str_num", ""+i);
			row.put("money", ""+(i-3));
			row.put("account_id", "account_"+i);
			
			Map ext = new HashMap();
			ext.put("ext_key", "ext_value_"+i % 3);
			ext.put("ext_num", i % 100);
			int random = RandomUtils.nextInt(3);
			for(int j = 0; j < random; j++) {
				ext.put("ext_"+i, j);
			}
			row.put("ext", ext);
			for(int j = 0; j < keyValuePairs.length; j+=2) {
				row.put(keyValuePairs[j], keyValuePairs[j+1]);
			}
			rows.add(row);
		}
		return rows.toArray(new HashMap[0]);
	}
	
	public static Object data(int index,Object... objects) {
		int i = index % objects.length;
		return objects[i];
	}

	public static List<Map> getTestDatasList(int count) {
		return Arrays.asList(getTestDatas(count));
	}
	
}
