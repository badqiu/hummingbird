package com.duowan.hummingbird.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.duowan.hummingbird.TestData;

public class MultiBirdDatabaseTest {
	BirdConnection conn = null;
	
	@Before
	public void setUp() throws Exception {
		//新建数据库
		MultiBirdDatabase db = new MultiBirdDatabase();
		db.newDatabase("blog_db"); 
		db.newDatabase("user_db"); 
		
		//初始化
		conn = db.newConnection();
		conn.select("use user_db", new HashMap());
		conn.insert("user",TestData.getTestDatasList(1000));
	}
	
	@Test
	public void test_success() throws Exception {
		//测试
		List<Map> select = conn.select("select * from user", new HashMap());
		assertEquals(1000,select.size());
		
	}
	
	@Test
	public void test_not_found_db(){
		try {
			conn.select("use not_exist_db", new HashMap());
			fail();
		}catch(IllegalArgumentException e) {
			assertEquals(e.getMessage(),"not found db by name:not_exist_db");
		}
	}
	
	@Test
	public void test_not_found_table(){
		conn.select("use blog_db", new HashMap());
		
		try {
			conn.select("select * from user", new HashMap());
			fail();
		}catch(RuntimeException e) {
			assertTrue(e.getMessage().startsWith("not found table by:user"));
		}
		
	}

}
