package com.duowan.hummingbird.gamma;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.duowan.hummingbird.db.aggr.BaseCountDistinct;
import com.duowan.hummingbird.db.aggr.BloomFilterCountDistinct;
import com.duowan.hummingbird.db.aggr.bloomfilter.HyperLogLogCountDistinct;

public class CountDistinctTest {

	
	@Before
	public void before() {
	}
	
	@Test
	public void test() {
		testWithDifAlgorithm(new BloomFilterCountDistinct());
		testWithDifAlgorithm(new HyperLogLogCountDistinct());
	}

	private void testWithDifAlgorithm(BaseCountDistinct cd) {
		List values = Arrays.asList(new String[]{"1","1","2","2","3","4","5","6","7","7"});
		double result = (Double)cd.exec( Arrays.asList(new Object[]{"groupBy"}), values,null);
		assertEquals(7,result,0);
		
		result = (Double)cd.exec( Arrays.asList(new Object[]{"groupBy"}), values,null);
		assertEquals(0,result,0);
		
		List values2 = Arrays.asList(new String[]{"1","1","2","2","3","4","5","6","7","7","8","9","10","10"});
		result = (Double)cd.exec( Arrays.asList(new Object[]{"groupBy"}), values2,null);
		assertEquals(3,result,0);
	}

}
