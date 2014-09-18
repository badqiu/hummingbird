package com.duowan.hummingbird.gamma.pool;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.duowan.realtime.thirft.api.Constants;

public class PartitionedHyperLogLog {
	
	private final static Logger LOG = LoggerFactory.getLogger(PartitionedHyperLogLog.class);
	
	private Map<String, HyperLogLog> partitions = new ConcurrentHashMap<String, HyperLogLog>();
	private String baseDir;
	private boolean changed = false;
	
	public PartitionedHyperLogLog(String baseDir) throws FileNotFoundException, ClassNotFoundException, IOException {
		Assert.hasText(baseDir,"baseDir must be not empty");
		this.baseDir = baseDir;
		recover();
	}
	
	public Map<String, HyperLogLog> getPartitons(){
		return partitions;
	}

	public HyperLogLog getHyperloglog(String partition) {
		changed = true;
		HyperLogLog r = partitions.get(partition);
		if(r == null) {
			r = new HyperLogLog(Constants.DEFAULT_NUMBER_BITS);
			LOG.info("create hyperLogLog instance" + "the number of bits to use:" + Constants.DEFAULT_NUMBER_BITS);
			partitions.put(partition, r);
		}
		return r;
	}
	
	public void dump() throws FileNotFoundException, IOException {
		if(!changed) {
			return;
		}
		changed = false;
		
		File file = dumpFile();
		file.getParentFile().mkdirs();
		ObjectOutputStream oos = null;
		LOG.info("start dump PartitionedHyperLogLog, file:"+file);
		try{
			long start = System.currentTimeMillis();
			oos = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
			for(Map.Entry<String, HyperLogLog> entry : partitions.entrySet()) {
				oos.writeObject(new HyperLogLogWraper(entry.getKey(), entry.getValue()));
			}
			long cost = System.currentTimeMillis() - start;
			LOG.info("dumped PartitionedHyperLogLog, file:"+file+" costSeconds:"+(cost/1000));
		}finally {
			IOUtils.closeQuietly(oos);
		}
	}
	
	public boolean isChanged() {
		return changed;
	}

	public void recover() throws FileNotFoundException, ClassNotFoundException, IOException {
		partitions = recover0();
	}
	
	private Map<String, HyperLogLog> recover0() throws FileNotFoundException, IOException, ClassNotFoundException {
		
		File file = dumpFile();
		if(!file.exists()) {
			return new ConcurrentHashMap<String, HyperLogLog>();
		}
		
		LOG.info("recover PartitionedHyperLogLog, file:"+file);
		ObjectInputStream ois = null;
		try{
			Map result = new ConcurrentHashMap();
			ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream(file)));
			HyperLogLogWraper w = null;
			try {
				while(true) {
					w = (HyperLogLogWraper)ois.readObject();
					result.put(w.key, HyperLogLog.Builder.build(w.hyperLogLogBytes));
				}
				
			} catch (EOFException e) {
				// TODO: handle exception
			}
			return result;
		}finally {
			IOUtils.closeQuietly(ois);
		}
	}

	private File dumpFile() {
		return new File(baseDir,"hyperloglog.dump");
	}
	
	public static class HyperLogLogWraper implements Serializable{
		
		private static final long serialVersionUID = -6078205760962257019L;
		String key;
		byte[] hyperLogLogBytes;
		public HyperLogLogWraper(String key, HyperLogLog hyperLogLog) throws IOException {
			super();
			this.key = key;
			this.hyperLogLogBytes = hyperLogLog.getBytes();
		}
	}
	
}
