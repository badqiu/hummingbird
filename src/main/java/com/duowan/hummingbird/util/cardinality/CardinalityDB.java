package com.duowan.hummingbird.util.cardinality;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;

public class CardinalityDB {
	private static Logger logger = LoggerFactory.getLogger(CardinalityDB.class);
	private Map<String, PartitionedCardinalityContainer> db = new ConcurrentHashMap<String, PartitionedCardinalityContainer>();
	private String baseDir = null;
	
	public CardinalityDB(String baseDir) {
		super();
		this.baseDir = baseDir;
		startDumpThread();
	}

	public CardinalityContainer getCardinalityContainer(String cardinalityName,String partition) {
		return getPartitionedCardinalityContainer(cardinalityName).get(partition);
	}
	
	public PartitionedCardinalityContainer getPartitionedCardinalityContainer(String cardinalityName) {
		PartitionedCardinalityContainer result = db.get(cardinalityName);
		if(result == null) {
			result = loadPartition(cardinalityName);
			db.put(cardinalityName, result);
		}
		return result;
	}

	private synchronized PartitionedCardinalityContainer loadPartition(String cardinalityName) {
		try {
			PartitionedCardinalityContainer p = new PartitionedCardinalityContainer(baseDir+"/"+cardinalityName);
			return p;
		}catch(Exception e) {
			throw new RuntimeException("load partition error",e);
		}
	}
	
	public void dump() {
		for(String groupName : db.keySet()) {
			PartitionedCardinalityContainer partitionedHyperLogLog = db.get(groupName);
			try {
				partitionedHyperLogLog.dump();
			}catch(Exception e) {
				//ignore
				logger.error("error on dump,groupName:"+groupName,e);
			}
		}
	}
	
	public void cleanNoChangeFromMemory() {
		for(String groupName : db.keySet()) {
			PartitionedCardinalityContainer partitionedHyperLogLog = db.get(groupName);
			try {
				partitionedHyperLogLog.cleanNoChangeFromMemory();
			}catch(Exception e) {
				//ignore
				logger.error("error on dump,groupName:"+groupName,e);
			}
		}
	}
	
	private void startDumpThread() {
		new Thread() {
			@Override
			public void run() {
				logger.info("start to backup hyperloglogplus and report status");
				//半小时备份？？
				long backupTime = 30 * 60 * 1000L;
				while (true) {
					try {
						Thread.sleep(backupTime);
						logger.info("time to backup every " + backupTime + " ms");
						cleanNoChangeFromMemory();
						dump();
					}catch(Exception e) {
						e.printStackTrace();
					}
				}
			}

		}.start();	
		
	}
	
}
