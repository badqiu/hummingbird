package com.duowan.hummingbird;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.duowan.hummingbird.db.BirdConnection;
import com.duowan.hummingbird.db.MultiBirdDatabase;

/**
 * 
 * @author chenwu
 */
public class DBDimTest {

	@Test
	public void test() throws Exception {
		MultiBirdDatabase db = new MultiBirdDatabase();
		db.newDatabase("zhgame");
		BirdConnection con = db.newConnection();
		con.select("use zhgame", null);
		con.insert("ods_action_log", getActionLogs());
		con.insert("dim_product", getDimProducts());
		String sql = "select product, game, game_server, ext, passport " +
				" into dwd_action_log " +
				" from ods_action_log a " +
				" join dim_product d on d.product=a.product ";
		List rows = con.select(sql, null);
		System.out.println(rows);
		TestUtil.assertContains(rows, "ext", newExtMap());
	}
	
	public List<Map> getActionLogs() {
		List<Map> list = new ArrayList<Map>();
		Map map = new HashMap();
		map.put("product", "yygame");
		map.put("game", "DDT");
		map.put("game_server", "s1");
		map.put("passport", "dw_chenwu");
		Map ext = newExtMap();
		map.put("ext", ext);
		list.add(map);
		
		map = new HashMap();
		map.put("product", "webyygame");
		map.put("game", "SMXJ");
		map.put("game_server", "s2");
		map.put("passport", "dw_xinwuyi");
		list.add(map);
		return list;
	}

	private Map newExtMap() {
		Map ext = new HashMap();
		ext.put("extk1", "extv1");
		ext.put("extk2", "extv2");
		ext.put("extk3", "extv3");
		return ext;
	}
	
	
	public List<Map> getDimProducts() {
		List<Map> list = new ArrayList<Map>();
		Map map = new HashMap();
		map.put("product", "yygame");
		map.put("create_time", new Date());
		map.put("create_user", "dw_taosheng");
		list.add(map);
		
		map = new HashMap();
		map.put("product", "webyygame");
		map.put("create_time", new Date());
		map.put("create_user", "dw_liuchaohong");
		list.add(map);
		return list;
	}
	
}