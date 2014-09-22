package com.duowan.hummingbird.util.cardinality;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionedCardinalityContainer {
	
	private final static Logger LOG = LoggerFactory.getLogger(PartitionedCardinalityContainer.class);
	
	private Map<String, CardinalityContainer> partitions = new ConcurrentHashMap<String, CardinalityContainer>();
	private String baseDir;

	public PartitionedCardinalityContainer(String baseDir) {
		super();
		this.baseDir = baseDir;
	}

	public CardinalityContainer get(String partition) {
		CardinalityContainer result = partitions.get(partition);
		if(result == null) {
			result = loadCardinalityContainer(partition);
		}
		return result;
	}
	
	private CardinalityContainer loadCardinalityContainer(String partition) {
		return null;
	}

	public void cleanNoChangeFromMemory() {
		for(String name : partitions.keySet()) {
			CardinalityContainer item = partitions.get(name);
			if(!item.isChanged()) {
				LOG.info("cleanNoChangePartitionedHyperLogLogFromMemory,name:"+name);
				partitions.remove(name);
			}
		}
	}
	
	public void dump() {
		for(CardinalityContainer cc : partitions.values()) {
			try {
				cc.dump();
			} catch (Exception e) {
				LOG.error("error when dump CardinalityContainer:"+cc,e);
			}
		}
	}
	
}
