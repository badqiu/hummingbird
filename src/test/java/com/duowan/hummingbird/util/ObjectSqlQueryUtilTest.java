package com.duowan.hummingbird.util;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.github.rapid.common.test.util.MultiThreadTestUtils;
import com.github.rapid.common.util.DateConvertUtil;

public class ObjectSqlQueryUtilTest {

	ObjectSqlQueryUtil util = new ObjectSqlQueryUtil();
	@Test
	public void test_build_sqls() {
		Map<String, Object> row = new HashMap();
		row.put("username", "badqiu");
		row.put("age", 100);
		row.put("percent", 12.239);
		row.put("birth_date", DateConvertUtil.parse("1999.9.9", "yyyy.MM.dd"));
		
		String createTableSql = util.buildCreateTableSql("t",Arrays.asList(row));
		String insertSql = util.buildInsertSql("t",row );
		System.out.println(createTableSql);
		System.out.println(insertSql);
		assertEquals("create memory local temporary  table t (birth_date datetime,percent DOUBLE,age INT,username varchar(4000) ) NOT PERSISTENT",createTableSql);
		assertEquals("insert into t (birth_date,percent,age,username ) values (:birth_date,:percent,:age,:username )",insertSql);
	}
	
	@Test
	public void testSelectSql() throws InterruptedException {
		final List list = genDataList();
		List<Map<String, Object>> selectResult = util.query("select username,sum(age) sum_age,min(age) min_age from t group by username", list);
		System.out.println(selectResult);
		assertEquals("[{min_age=100, sum_age=500, username=jane}, {min_age=100, sum_age=100, username=badqiu}]",selectResult.toString());
	
		selectResult = util.query("select username,get_property(map,'sex') sex,get_property(user,'age') age from t group by username", list);
		System.out.println(selectResult);
		assertEquals("[{sex=F, username=jane, age=10}, {sex=F, username=badqiu, age=10}]",selectResult.toString());
	}
	
	@Test
	public void testCreateSql() {
		
	}

	private List genDataList() {

		Map testMap = MapUtil.newMap("sex","F","age",100);
		TestUser user = new TestUser("qq",10);
		
		final List list = new ArrayList();
		list.add(MapUtil.newMap("username","badqiu","age",100,"percent",12.239,"birth_date", DateConvertUtil.parse("1999.9.9", "yyyy.MM.dd"),"money",null,"map",testMap,"user",user));
		list.add(MapUtil.newMap("username","jane","age",100,"percent",10.1,"birth_date", DateConvertUtil.parse("1999.9.9", "yyyy.MM.dd"),"money",100,"map",testMap,"user",user));
		list.add(MapUtil.newMap("username","jane","age",200,"percent",10.1,"birth_date", DateConvertUtil.parse("1999.9.9", "yyyy.MM.dd"),"money",100,"map",testMap,"user",user));
		list.add(MapUtil.newMap("username","jane","age",200,"percent",10.1,"birth_date", DateConvertUtil.parse("1999.9.9", "yyyy.MM.dd"),"money",1000,"map",testMap,"user",user));
		return list;
	}
	
	public static class TestUser implements Serializable {
		private String username;
		private int age;
		public String getUsername() {
			return username;
		}
		public void setUsername(String username) {
			this.username = username;
		}
		public int getAge() {
			return age;
		}
		public void setAge(int age) {
			this.age = age;
		}
		
		public TestUser(String username, int age) {
			super();
			this.username = username;
			this.age = age;
		}
		
	}

	@Test
	public void testMultiDataSet() throws InterruptedException {
		final List list = genDataList();
//		util.query("select username,sum(age) sum_age,min(age) min_age from t1 group by username", list,list);
	}
	

	@Test
	public void testPerf() throws InterruptedException {
		final List list = new ArrayList();
		for(int i = 0; i < 50000; i++) {
			list.addAll(genDataList());
		}
		final int count = 100;
		long start = System.currentTimeMillis();
		int threads = 4;
		MultiThreadTestUtils.executeAndWait(threads, new Runnable() {
			@Override
			public void run() {
				for(int i = 0; i < count; i++) {
					util.query("select username,sum(age) sum_age,min(age) min_age from table_info group by username", list);
				}				
			}
		});
		long cost = System.currentTimeMillis() - start;
		System.out.println("cost:"+cost+" tps:"+(threads * list.size() + count * 1000.0 / cost));
//		System.out.println(util.query("select username,collect_map(map(username,age)) ext from t group by username",list));
	}
	
	public static void main(String[] args) throws InterruptedException {
		new ObjectSqlQueryUtilTest().testPerf();
	}

}
