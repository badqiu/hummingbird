package com.duowan.hummingbird.util.bloomfilter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 
 * bloomfilter数据库,
 * 采用 bloomfilter db => bloomfilter partition => bloomfilter instance三级结构
 * 类似mysql的 db => table => partition结构
 * 
 * @author badqiu
 *
 */
public class BloomFilterDB {

	private static final Logger logger = LoggerFactory.getLogger(BloomFilterDB.class);
	private static final int DEFAULT_DUMP_INTERVAL_SECONDS = 3600;
	
	private String baseDir = null;
	private int dumpIntervalSeconds = DEFAULT_DUMP_INTERVAL_SECONDS;
	
	private Map<String,PartitionBloomFilter> db = new ConcurrentHashMap<String, PartitionBloomFilter>();
	
	public BloomFilterDB(String baseDir) {
		this(baseDir,DEFAULT_DUMP_INTERVAL_SECONDS);
	}
	
	public BloomFilterDB(String baseDir,int dumpIntervalSeconds) {
		super();
		this.baseDir = baseDir;
		this.dumpIntervalSeconds = dumpIntervalSeconds;
		startDumpThread();
	}

	public PartitionBloomFilter get(String bloomFilterName) {
		PartitionBloomFilter result = db.get(bloomFilterName);
		if(result == null) {
			synchronized (this) {
				String bloomFilterDir = baseDir + "/" + bloomFilterName;
				result = new PartitionBloomFilter(bloomFilterDir);
				db.put(bloomFilterName,result);
			}
		}
		return result;
	}
	
	public BloomFilter get(String bloomFilterName,String partition) {
		return get(bloomFilterName).getBloomFilter(partition);
	}
	
	public void dump() {
		logger.info("start BloomFilterDB dump()");
		for(Map.Entry<String, PartitionBloomFilter> entry : db.entrySet()) {
			String db = entry.getKey();
			PartitionBloomFilter bf = entry.getValue();
			try {
				bf.dump();
			} catch (Exception e) {
				logger.error("dump error",e);
			}
		}
	}
	
	public void clearAllDBNoChangeBloomFilter() {
		logger.info("start clearAllDBNoChangeBloomFilter()");
		for(Map.Entry<String, PartitionBloomFilter> entry : db.entrySet()) {
			String db = entry.getKey();
			PartitionBloomFilter bf = entry.getValue();
			bf.clearNoChangeBloomFilter();
		}
	}
	
	public void startDumpThread() {
		Thread t = new Thread("BloomFilterDB-dump"){
			public void run() {
				while(true) {
					try {
						Thread.sleep(1000 * dumpIntervalSeconds);
						clearAllDBNoChangeBloomFilter();
						dump();
					}catch(Exception e) {
						logger.error("dump thread task error",e);
					}
				}
			}
		};
		t.start();
	}
	
}
