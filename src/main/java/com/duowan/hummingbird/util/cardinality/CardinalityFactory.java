package com.duowan.hummingbird.util.cardinality;

import com.clearspring.analytics.stream.cardinality.ICardinality;

public interface CardinalityFactory {

	public ICardinality create();

	public Object recover(byte[] bytes);
	
}
