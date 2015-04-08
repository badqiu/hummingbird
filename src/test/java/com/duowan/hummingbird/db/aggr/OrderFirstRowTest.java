package com.duowan.hummingbird.db.aggr;

import static com.duowan.hummingbird.TestUtil.printRows;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

import com.duowan.hummingbird.TestData;
import com.duowan.hummingbird.db.BirdDatabase;
import com.duowan.realtime.computing.HyperLogLogClient;
import com.yy.distinctservice.client.BloomFilterClientProvider;

public class OrderFirstRowTest {

	BirdDatabase db = new BirdDatabase();
	
	@Before
	public void before() throws Exception {
		CountDistinctProviderImpl provider = new CountDistinctProviderImpl();
		provider.setBloomFilterClient(new BloomFilterClientProvider());
		provider.setHyperLogLogClient(new HyperLogLogClient());
		BirdDatabase.setCountDistinctProvider(provider);
		
		db.insert("user", Arrays.asList(TestData.getTestDatas(10)));
		db.insert("dim_user", Arrays.asList(TestData.getTestDatas(20,"diy_key","diy_value")));
	}
	
	@Test
	public void testExecByBatch() throws Exception {
		printRows(db.select("select * from user "));
		//order_first_row for date compare
		System.out.println(" --- order_first_row for date compare --- ");
		printRows(db.select("select game,game_server,order_first_row('desc',stime,extract(parse('20141121','yyyyMMdd'),'yyyyMMdd'), passport,id,dur,ext) AS ext_columns from user group by game_server"));
		printRows(db.select("select game_server,order_first_row('asc',stime,extract(parse('20141121','yyyyMMdd'),'yyyyMMdd'),passport,game,id,dur,ext) AS ext_columns from user group by game_server"));
		//order_first_row for Number compare
		System.out.println(" --- order_first_row for Number compare --- ");
		printRows(db.select("select game_server,order_first_row('desc',dur,stime,passport,game,id,ext) AS ext_columns from user group by game_server"));
		printRows(db.select("select game_server,order_first_row('asc',dur,stime,passport,game,id,ext) AS ext_columns from user group by game_server"));
	
		//order_first_row for String compare
		System.out.println(" --- order_first_row for String compare --- ");
		printRows(db.select("select game_server,order_first_row('desc',game,dur,stime,passport,id,dur,ext) AS ext_columns from user group by game_server"));
		printRows(db.select("select game_server,order_first_row('asc',game,dur,stime,passport,id,dur,ext) AS ext_columns from user group by game_server"));
		
		
	
	}

}
