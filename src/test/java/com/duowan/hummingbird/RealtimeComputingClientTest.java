package com.duowan.hummingbird;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.junit.Test;

import com.duowan.realtime.computing.BloomFilterClient;
import com.duowan.realtime.computing.HyperLogLogClient;
import com.duowan.realtime.thirft.api.HyperLogLogPlusException;
import com.duowan.realtime.thirft.api.HyperLogLogQuery;

public class RealtimeComputingClientTest {

	@Test
	public void test() throws HyperLogLogPlusException, TException {
		BloomFilterClient c = new BloomFilterClient();
		
		Date bloomfilterGroups;
		String stime;
		String group;
		List<String> values;
		
//		c.notContainsCountAndAdd(bloomfilterGroups,stime, group, values);
		
		HyperLogLogClient hc = new HyperLogLogClient();
		List<HyperLogLogQuery> hllpQueryList = new ArrayList<HyperLogLogQuery>();
		HyperLogLogQuery e = new HyperLogLogQuery();
		e.setGroup("blogjava");
		e.setValues(Arrays.asList("1","2","3","4","5","6","7","8","9","0","1"));
		hllpQueryList.add(e);
		Map<String,Integer> map = hc.offerForCardinality("day", hllpQueryList );
		System.out.println(map);
	}
}
