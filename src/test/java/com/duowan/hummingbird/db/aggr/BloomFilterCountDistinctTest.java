package com.duowan.hummingbird.db.aggr;

import static com.duowan.hummingbird.TestUtil.printRows;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

import com.duowan.hummingbird.TestData;
import com.duowan.hummingbird.db.BirdDatabase;
import com.duowan.hummingbird.db.aggr.bloomfilter.CountDistinctProviderImpl;
import com.github.distinct_server.client.DistinctServiceClient;

public class BloomFilterCountDistinctTest {

	BirdDatabase db = new BirdDatabase();
	
	@Before
	public void before() throws Exception {
		CountDistinctProviderImpl provider = new CountDistinctProviderImpl();
		DistinctServiceClient bloomFilterClient = new DistinctServiceClient();
		bloomFilterClient.setHost("localhost");
		bloomFilterClient.setVhost("default_vhost");
		bloomFilterClient.afterPropertiesSet();
//		MetaStoreWebService metaStoreWebService = Mockito.mock(MetaStoreWebService.class);
//		Mockito.when(metaStoreWebService.getServerIp(Mockito.anyString())).thenReturn("127.0.0.1") ; 
//		BloomFilterClientFactory.setMetaStoreWebService(metaStoreWebService);
		provider.setDistinctServiceClient(bloomFilterClient);
//		provider.setHyperLogLogClient(new HyperLogLogClient());
		BirdDatabase.setCountDistinctProvider(provider);
		
		db.insert("user", Arrays.asList(TestData.getTestDatas(10)));
		db.insert("dim_user", Arrays.asList(TestData.getTestDatas(20,"diy_key","diy_value")));
	}
	
	@Test
	public void testExecByBatch() throws Exception {
		printRows(db.select("select * from user "));
		//order_first_row for date compare
		System.out.println(" --- bf_count_distinct --- ");
		printRows(db.select("select game,game_server, sum(dur),bf_count_distinct(passport,'minute_bf',format(parse('20141121','yyyyMMdd'),'yyyyMMdd')) as distinct_count from user   group by  'sum_new_user_ucnt',extract(parse('1970-01-01', 'yyyy-MM-dd'),'yyyyMMdd'),game,game_server"));
		System.out.println(" --- bf_count_distinct --- ");
		printRows(db.select("select game,game_server,count(dur),sum(dur),bf_count_distinct(passport,'minute_bf',format(stime,'yyyyMMdd')) as distinct_count from user where game != 'as' group by format(stime,'yyyyMMdd'),game,game_server"));
		
	}

}
