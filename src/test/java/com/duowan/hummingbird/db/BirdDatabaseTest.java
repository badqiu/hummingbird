package com.duowan.hummingbird.db;

import static com.duowan.hummingbird.TestUtil.assertContains;
import static com.duowan.hummingbird.TestUtil.printRows;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mvel2.PropertyAccessException;

import com.duowan.common.util.DateConvertUtils;
import com.duowan.hummingbird.TestData;
import com.duowan.hummingbird.db.aggr.CountDistinctProviderImpl;
import com.duowan.realtime.computing.BloomFilterClient;
import com.duowan.realtime.computing.HyperLogLogPlusClient;

public class BirdDatabaseTest {

	BirdDatabase db = new BirdDatabase();
	
	@Before
	public void before() throws Exception {
		CountDistinctProviderImpl provider = new CountDistinctProviderImpl();
		provider.setBloomFilterClient(new BloomFilterClient());
		provider.setHyperLogLogPlusClient(new HyperLogLogPlusClient());
		BirdDatabase.setCountDistinctProvider(provider);
		
		db.insert("user", Arrays.asList(TestData.getTestDatas(10)));
		db.insert("dim_user", Arrays.asList(TestData.getTestDatas(20,"diy_key","diy_value")));
	}
	
	@Test
	public void test() throws Exception {
		printRows(db.select("select game,game_server,count(dur),sum(dur),count_distinct(passport),hll_count_distinct(passport) from user where game != 'as' group by game,game_server"));
		printRows(db.select("select count(dur),sum(dur),count_distinct(passport),hll_count_distinct(passport) from user where game != 'as' group by game,game_server"));
		printRows(db.select("select diy_key,extract(stime,'yyyyMMdd') as tdate,game,game_server,count(dur),sum(dur),count_distinct(passport) from user u join dim_user du on u.game = du.game where game != 'as' group by diy_key,extract(stime,'yyyyMMdd'),game,game_server having game = 'hz' "));
		printRows(db.select("select id,diy_key,extract(stime,'yyyyMMdd') as tdate,game,game_server,dur,passport from user u join dim_user du on u.game = du.game where game != 'as' limit 0,2"));
		printRows(db.select("select id,extract(stime,'yyyyMMdd') as tdate,game,game_server,dur,passport from user order by id asc limit 2,3"));
		
		printRows(db.select("select id,'sub select' subselect,game,game_server,dur,passport from (select id,stime,game,game_server,dur,passport from user) t  order by id asc limit 2,3"));
	}
	
	@Test(expected=PropertyAccessException.class)
	public void method_not_exist() throws Exception {
		printRows(db.select("select game,game_server,no_exist_method(game) from user where game != 'as' "));
	}
	
	@Test
	public void where() throws Exception {
		List<Map> rows = db.select("select * from user where game != 'as' and dur>5 and game!='ddt'");
		assertContains(rows,"id",8);
		assertTrue(rows.size() == 1);
		rows = db.select("select * from user where id=6 and game != 'as' and dur>=5 and dur <=100 and game='ddt' or 1!=1");
		printRows(rows);
		assertContains(rows,"game","ddt");
		assertContains(rows,"id",6);
		assertEquals(rows.size(),1);
	}
	
	@Test
	public void aggr_collect_map() {
		List<Map> rows = db.select("select game,collect_map(ext) from dim_user group by game");
		printRows(rows);
	}
	
	@Test
	public void stringConcat() throws Exception {
		List<Map> rows = db.select("select 'abc'+'123' stringconcat, 'number'+455 numberconcat,8+100 numberexpr from user where game != 'as' and dur>5 and game!='ddt'");
		printRows(rows);
		assertEquals(rows.size(),1);
		assertContains(rows,"stringconcat","abc123");
		assertContains(rows,"numberconcat","number455");
		assertContains(rows,"numberexpr",108);
	}
	
	@Test
	public void subSelect() throws Exception {
		List<Map> rows = db.select("select diy_key,id,'sub select' subselect,game,game_server,dur,passport from (select id,stime,game,game_server,dur,passport from user) t inner join (select * from dim_user) t1 on t.game=t1.game  order by id asc limit 2,3");
		printRows(rows);
		assertContains(rows,"id",1,0);
		assertContains(rows,"subselect","sub select");
		assertContains(rows,"diy_key","diy_value");
	}
	
	@Test
	public void ext_map_data_type() throws Exception {
		List<Map> rows = db.select("select ext.ext_key,sum(ext.ext_num) from user group by ext.ext_key");
		printRows(rows);
		assertEquals(rows.size(),3);
		assertContains(rows,"sum_ext.ext_num",18.0,15.0,12.0);
		assertContains(rows,"ext.ext_key","ext_value_0","ext_value_2","ext_value_1");
		
		
		rows = db.select("select ext.ext_key,ext.ext_num from user limit 5");
		printRows(rows);
		assertContains(rows,"ext.ext_num",1,2,3,4);
		assertContains(rows,"ext.ext_key","ext_value_0","ext_value_2","ext_value_1");
		assertEquals(rows.size(),5);
		
	}
	
	@Test
	public void aggr() throws Exception {
		{
			List<Map> rows = db.select("select game,count(dur),sum(dur),max(dur),min(passport) from user where game != 'as' group by game");
			printRows(rows);
			assertEquals(rows.size(),2);
			assertContains(rows,"max_dur",9,8);
			assertContains(rows,"count_dur",4,3);
			assertContains(rows,"min_passport",0,2);
			assertContains(rows,"sum_dur",18.0,15.0);
			assertContains(rows,"game","ddt","hz");
		}
		{
			List<Map> rows = db.select("select game,count(dur),sum(dur),count_distinct(passport,'day') from user where game != 'as' group by game");
			printRows(rows);
			assertContains(rows,"game","ddt","hz");
			assertContains(rows,"count_dur",4.0,3.0);
			assertContains(rows,"sum_dur",18.0,15.0);
		}
		
	}
	
	@Test
	public void having() {
		List<Map> rows = db.select("select game,sum(dur)  from user group by game having sum_dur > 16"); 
		printRows(rows);
		assertEquals(rows.size(),1);
		assertContains(rows,"game","ddt");
	}
	
	@Test
	public void groupBy() throws Exception {
		List<Map> rows = db.select("select diy_key,extract(stime,'yyyyMMdd') as tdate,game,game_server,count(dur),sum(dur) from user u join dim_user du on u.game = du.game where game != 'as' group by diy_key,extract(stime,'yyyyMMdd'),game,game_server having game = 'hz' ");
		printRows(rows);
		assertContains(rows,"diy_key","diy_value");
		assertContains(rows,"tdate",DateConvertUtils.parse("1999-1-1", "yyyy-MM-dd"),DateConvertUtils.parse("1999-1-2", "yyyy-MM-dd"),DateConvertUtils.parse("1999-1-3", "yyyy-MM-dd"));
		assertContains(rows,"sum_dur",48.0,66.0,24.0);
	}
	
	@Test
	public void testAggrWithAttatchParams() throws Exception {
		List<Map> rows = db.select("select game,game_server,count(dur,'abc',123),sum(dur) from user  group by game,game_server  ");
		
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testNoSuchAggrFunction() throws Exception {
		List<Map> rows = db.select("select game,not_exist_aggr_func(passport) from user  group by game");
		printRows(rows);
	}
	
	
//	@Test
//	public void testPerf() throws Exception {
//		int count = 10000;
//		for(int dataSize : new int[]{100,10000,100000}) {
//			long start = System.currentTimeMillis();
//			db.insert("user", TestData.getTestDatasList(dataSize));
//			for(int i = 0; i < count; i++) {
//				db.select("select count(dur),sum(dur),count_distinct(passport) from user where game != 'as' group by game,game_server");
//			}
//			long cost = System.currentTimeMillis() - start;
//			System.out.println("perf, cost:"+cost+" tps:"+(count * 1000.0 / cost)+" count:"+count+" dataSize:"+dataSize+" rows_per_sencond:"+(count*dataSize*1000.0 / cost));
//			db.truncate("user");
//		}
//		
//	}
	
	@Test
	public void insertInto() throws Exception {
		db.select("select id,extract(stime,'yyyyMMdd') as tdate,game,game_server,dur,passport Into user_copy   from user  order by id asc limit 2,3  ");
		assertNotNull(db.getTable("user_copy"));
	}
	
	@Test
	public void orderBy_and_limit() throws Exception {
		List<Map> rows = db.select("select id,extract(stime,'yyyyMMdd') as tdate,game,game_server,dur,passport into user_copy from user  order by id asc limit 2,3  ");
		assertContains(rows,"id",2,3,4);
		
		rows = db.select("select id,extract(stime,'yyyyMMdd') as tdate,game,game_server,dur,passport into user_copy from user  order by id desc limit 2,3  ");
		assertContains(rows,"id",7,5,6);
	}
	
	@Test
	public void allTableColumns() throws Exception {
		List<Map> rows = db.select("select * from user");
		printRows(rows);
		assertContains(rows,"id",1,2,3);
		assertContains(rows,"game","ddt","hz");
	}

	@Test
	public void test_not_table() throws Exception {
		try {
			System.out.println(db.select("select count(dur),sum(dur) from not_exist_user where game != 'as' group by game,game_server"));
			fail("error");
		}catch(RuntimeException e) {
			assertTrue(e.getMessage(),e.getMessage().equals("not found table by:not_exist_user as null"));
		}
	}

    @Test
    public void testRefference() throws Exception{
        Map[]  maps = TestData.getTestDatas(10);
        List<Map> myList = new ArrayList<Map>();
        for (int i = 0; i < maps.length; i++) {
            myList.add(maps[i]);
        };
        db.insert("myList",myList);
        myList.add(new HashMap(){{put("name","jacky");}});
        List<Map> myTable = db.getTable("myList");
        assertNull(myTable.get(myTable.size()-1).get("name"));
    }

}
