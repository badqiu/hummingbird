package com.duowan.hummingbird.db.aggr;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.apache.commons.collections.ComparatorUtils;
import org.junit.Test;

public class MaxTest {

	@Test
	public void test() {
		Collection querys = new ArrayList();
		querys.add(1);
		querys.add(2);
		querys.add(1000);
		
		Object max = Collections.max(querys ,ComparatorUtils.NATURAL_COMPARATOR);
		assertEquals(1000,max);
		
		max = Collections.max(new ArrayList() ,ComparatorUtils.NATURAL_COMPARATOR);
		assertEquals(1000,max);
	}

}
