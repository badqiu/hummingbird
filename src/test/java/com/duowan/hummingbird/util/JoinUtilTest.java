package com.duowan.hummingbird.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

/**
 * 
 * @author chenwu
 */
public class JoinUtilTest {

	@Test
	public void testInnerJoin() {
		String joinCondition = "d.game = p.game and d.game_server=p.game_server";
		String leftAlias = "d";
		String rightAlias = "p";
		List<Map> leftList = initLeftList();
		List<Map> rightList = initRightList();
		List<Map> resultList = JoinUtil.innerJoin(leftAlias, leftList, rightAlias, rightList, joinCondition);
		Assert.assertTrue(resultList.size() == 1);
		
		try {
			JoinUtil.innerJoin(leftAlias, leftList, rightAlias, rightList, "d.game = p.game or d.game_server>p.game_server");
			Assert.assertTrue(false);
		} catch(Exception e) {
			e.printStackTrace();
			Assert.assertTrue(true);
		}
	}
	
	@Test
	public void testLeftJoin() {
		String joinCondition = "d.game = p.game and d.game_server=p.game_server";
		String leftAlias = "d";
		String rightAlias = "p";
		List<Map> leftList = initLeftList();
		List<Map> rightList = initRightList();
		List<Map> resultList = JoinUtil.leftJoin(leftAlias, leftList, rightAlias, rightList, joinCondition);
		Assert.assertTrue(resultList.size() == 3);
	}
	
	@Test
	public void testRightJoin() {
		String joinCondition = "d.game = p.game and d.game_server=p.game_server";
		String leftAlias = "d";
		String rightAlias = "p";
		List<Map> leftList = initLeftList();
		List<Map> rightList = initRightList();
		List<Map> resultList = JoinUtil.rightJoin(leftAlias, leftList, rightAlias, rightList, joinCondition);
		Assert.assertTrue(resultList.size() == 3);
	}
	
	private List<Map> initLeftList() {
		List<Map> rows = new ArrayList<Map>();
		Map dataMap;
		dataMap = new HashMap();
		dataMap.put("game", "DDT");
		dataMap.put("game_server", "s1");
		dataMap.put("duowanb", 100);
		rows.add(dataMap);
		
		dataMap = new HashMap();
		dataMap.put("game", "DDT");
		dataMap.put("game_server", "s2");
		dataMap.put("duowanb", 100);
		rows.add(dataMap);
		
		dataMap = new HashMap();
		dataMap.put("game", "DDT");
		dataMap.put("game_server", "s3");
		dataMap.put("duowanb", 100);
		rows.add(dataMap);
		return rows;
	}
	
	private List<Map> initRightList() {
		List<Map> rows = new ArrayList<Map>();
		Map dataMap;
		dataMap = new HashMap();
		dataMap.put("game", "DDT");
		dataMap.put("game_server", "s1");
		rows.add(dataMap);
		
		dataMap = new HashMap();
		dataMap.put("game", "DDT");
		dataMap.put("game_server", "s4");
		rows.add(dataMap);
		
		dataMap = new HashMap();
		dataMap.put("game", "DDT");
		dataMap.put("game_server", "s5");
		rows.add(dataMap);
		return rows;
	}
}
