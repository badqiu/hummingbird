package com.duowan.hummingbird.gamma.pool;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;

public class HyperLogLogDB {
	private static Logger logger = LoggerFactory.getLogger(HyperLogLogDB.class);
	private Map<String, PartitionedHyperLogLog> db = new ConcurrentHashMap<String, PartitionedHyperLogLog>();
	private String baseDir = null;
	
	public HyperLogLogDB(String baseDir) {
		super();
		this.baseDir = baseDir;
		startDumpThread();
	}

	public PartitionedHyperLogLog getPartitionedHyperLogLog(String hllName) {
		PartitionedHyperLogLog result = db.get(hllName);
		if(result == null) {
			result = loadPartition(hllName);
			db.put(hllName, result);
		}
		return result;
	}

	private synchronized PartitionedHyperLogLog loadPartition(String hllName) {
		try {
			PartitionedHyperLogLog p = new PartitionedHyperLogLog(baseDir+"/"+hllName);
			return p;
		}catch(Exception e) {
			throw new RuntimeException("load partition error",e);
		}
	}
	
	public HyperLogLog getHyperLogLogIfNotExistAndInit(String hllName, String partition){
		PartitionedHyperLogLog bf = getPartitionedHyperLogLog(hllName);
		return bf.getHyperloglog(partition);
	}
	
	public void dump() {
		for(String groupName : db.keySet()) {
			PartitionedHyperLogLog partitionedHyperLogLog = db.get(groupName);
			try {
				partitionedHyperLogLog.dump();
			}catch(Exception e) {
				//ignore
				logger.error("error on dump,groupName:"+groupName,e);
			}
		}
	}
	
	public void cleanNoChangePartitionedHyperLogLogFromMemory() {
		for(String groupName : db.keySet()) {
			PartitionedHyperLogLog partitionedHyperLogLog = db.get(groupName);
			if(!partitionedHyperLogLog.isChanged()) {
				logger.info("cleanNoChangePartitionedHyperLogLogFromMemory,groupName:"+groupName);
				db.remove(groupName);
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
						cleanNoChangePartitionedHyperLogLogFromMemory();
						dump();
					}catch(Exception e) {
						e.printStackTrace();
					}
				}
			}

		}.start();	
		
	}
	
}
