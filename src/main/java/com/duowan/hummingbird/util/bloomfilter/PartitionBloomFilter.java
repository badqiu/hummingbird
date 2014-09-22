package com.duowan.hummingbird.util.bloomfilter;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

public class PartitionBloomFilter {
	private static Logger logger = LoggerFactory.getLogger(PartitionBloomFilter.class);
	
	private String baseDir;
	private Map<String,BloomFilter> partitions = new HashMap();
	
	public PartitionBloomFilter(String baseDir) {
		super();
		this.baseDir = baseDir;
	}

	public BloomFilter getBloomFilter(String partition) {
		BloomFilter bf = partitions.get(partition);
		if(bf == null) {
			try {
				bf = loadPartition(partition);
				partitions.put(partition,bf);
			}catch(Exception e) {
				throw new RuntimeException("error on load partition:"+partition,e);
			}
		}
		return bf;
	}

	private BloomFilter loadPartition(String partition) throws FileNotFoundException, IOException, ClassNotFoundException {
		Assert.hasText(partition,"partition must be not empty");
		File file = partitionFile(partition);
		if(file.exists()) {
			ObjectInputStream ois = null;
			try {
				ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream(file),128 * 1024));
				return (BloomFilter)ois.readObject();
			}finally {
				IOUtils.closeQuietly(ois);
			}
		}else {
			return new BloomFilter(Integer.MAX_VALUE, Integer.MAX_VALUE);
		}
	}

	public void dump() throws FileNotFoundException, IOException {
		for(String partition : partitions.keySet()) {
			try {
				long start = System.currentTimeMillis();
				dump(partition);
				long cost = System.currentTimeMillis() - start;
				logger.info("dumped partition cost_seconds:"+(cost/1000)+" partition:"+partition);
			}catch(Exception e) {
				logger.error("dump error",e);
			}
		}
	}

	/**
	 * 清除没有任何修改的BloomFilter 出内存
	 */
	public void clearNoChangeBloomFilter() {
		Set<String> keySet = partitions.keySet();
		for(String partition : keySet) {
			BloomFilter bf = partitions.get(partition);
			if(bf.isChange()){
				continue;
			}
			partitions.remove(partition);
		}
	}
	
	private void dump(String partition) throws IOException,
			FileNotFoundException {
		BloomFilter bf = partitions.get(partition);
		if(bf.isChange()) {
			bf.cleanChange();
			
			File file = partitionFile(partition);
			file.getParentFile().mkdirs();
			
			logger.info("start PartitionBloomFilter dump(), file:"+file);
			ObjectOutputStream oos = null;
			try {
				oos = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(file),128 * 1024));
				oos.writeObject(bf);
			}finally {
				IOUtils.closeQuietly(oos);
			}
		}
	}

	private File partitionFile(String partition) {
		return new File(baseDir,partition+".bloomfilter");
	}

}
