package com.duowan.hummingbird.util.bloomfilter;

import static org.junit.Assert.*;

import org.junit.Test;

import com.github.rapid.common.test.util.MultiThreadTestUtils;
import com.github.rapid.common.util.Profiler;

public class BloomFilterTest {

	@Test
	public void test() {
		BloomFilter bf = new BloomFilter(0.001,Integer.MAX_VALUE);
		System.out.println(bf);
		
		int count = 1000000;
		Profiler.start("add");
		for(int i = 0; i < count; i++) {
			bf.add(String.valueOf(i));
		}
		Profiler.release(count);
		System.out.println(Profiler.dump());
		
		Profiler.start("contains");
		for(int i = 0; i < count; i++) {
			bf.contains(String.valueOf(i));
		}
		Profiler.release(count);
		
		System.out.println(Profiler.dump());
	}

}
