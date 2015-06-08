package com.duowan.hummingbird.gamma.sqlparser;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.mvel2.MVEL;

import com.github.rapid.common.util.Profiler;

public class SqlParserTest {


	@Test
	public void test_mvel_perf() {
		Map map = new HashMap();
		map.put("username", "badqiu");
		long count = 1;
		Serializable expr = MVEL.compileExpression("[username,username.substring(1,2)]");
		Profiler.start("mvel_perf",count);
		for(int i = 0; i < count; i++) {
			Object v = MVEL.executeExpression(expr,map);
			System.out.println(v);
		}
		Profiler.release();
		System.out.println(Profiler.dump());
		
	}
}
