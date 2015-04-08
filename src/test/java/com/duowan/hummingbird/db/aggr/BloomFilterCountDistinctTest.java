package com.duowan.hummingbird.db.aggr;

import static com.duowan.hummingbird.TestUtil.printRows;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.duowan.common.util.DateConvertUtils;
import com.duowan.hummingbird.TestData;
import com.duowan.hummingbird.db.BirdDatabase;
import com.duowan.realtime.computing.BloomFilterClient;
import com.duowan.realtime.computing.HyperLogLogClient;
import com.duowan.realtime.webservice.DataComputingWebService;
import com.yy.distinctservice.client.BloomFilterClientFactory;
import com.yy.distinctservice.client.BloomFilterClientProvider;
import com.yy.distinctservice.metastore.webservice.MetaStoreWebService;

public class BloomFilterCountDistinctTest {

	BirdDatabase db = new BirdDatabase();
	
	@Before
	public void before() throws Exception {
		CountDistinctProviderImpl provider = new CountDistinctProviderImpl();
		BloomFilterClientProvider bloomFilterClient = new BloomFilterClientProvider();
//		MetaStoreWebService metaStoreWebService = Mockito.mock(MetaStoreWebService.class);
//		Mockito.when(metaStoreWebService.getServerIp(Mockito.anyString())).thenReturn("127.0.0.1") ; 
//		BloomFilterClientFactory.setMetaStoreWebService(metaStoreWebService);
		provider.setBloomFilterClient(bloomFilterClient);
		provider.setHyperLogLogClient(new HyperLogLogClient());
		BirdDatabase.setCountDistinctProvider(provider);
		
		db.insert("user", Arrays.asList(TestData.getTestDatas(10)));
		db.insert("dim_user", Arrays.asList(TestData.getTestDatas(20,"diy_key","diy_value")));
	}
	
	@Test
	public void testExecByBatch() throws Exception {
		printRows(db.select("select * from user "));
		//order_first_row for date compare
		System.out.println(" --- bf_count_distinct --- ");
		printRows(db.select("select game,game_server, sum(dur),bf_count_distinct(passport,'test_db_01','minute',format(parse('20141121','yyyyMMdd'),'yyyyMMdd')) as distinct_count from user   group by  'sum_new_user_ucnt',extract(parse('1970-01-01', 'yyyy-MM-dd'),'yyyyMMdd'),game"));
		printRows(db.select("select game,game_server,count(dur),sum(dur),bf_count_distinct(passport,'test_db_01','minute',format(stime,'yyyyMMdd')) as distinct_count,cardinality_offer(passport,'day') from user where game != 'as' group by format(stime,'yyyyMMdd'),game,game_server"));
		
	}

}
