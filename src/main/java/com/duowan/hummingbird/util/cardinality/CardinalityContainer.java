package com.duowan.hummingbird.util.cardinality;

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
import com.clearspring.analytics.stream.cardinality.ICardinality;

public class CardinalityContainer {
	private final static Logger LOG = LoggerFactory.getLogger(PartitionedCardinalityContainer.class);
	
	private Map<String, ICardinality> partitions = new ConcurrentHashMap<String, ICardinality>();
	private String baseDir;
	private boolean changed = false;
	
	public CardinalityContainer(String baseDir) throws FileNotFoundException, ClassNotFoundException, IOException {
		Assert.hasText(baseDir,"baseDir must be not empty");
		this.baseDir = baseDir;
		recover();
	}
	
	public Map<String, ICardinality> getPartitons(){
		return partitions;
	}

	public ICardinality getCardinality(String key) {
		changed = true;
		ICardinality r = partitions.get(key);
		if(r == null) {
			int log2m = 25;
			r = new HyperLogLog(log2m);
			LOG.info("create hyperLogLog instance" + "the number of bits to use:" + log2m);
			partitions.put(key, r);
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
		LOG.info("start dump CardinalityContainer, file:"+file);
		try{
			long start = System.currentTimeMillis();
			oos = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
			for(Map.Entry<String, ICardinality> entry : partitions.entrySet()) {
				oos.writeObject(new ICardinalityWraper(entry.getKey(), entry.getValue()));
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
	
	private Map<String, ICardinality> recover0() throws FileNotFoundException, IOException, ClassNotFoundException {
		
		File file = dumpFile();
		if(!file.exists()) {
			return new ConcurrentHashMap<String, ICardinality>();
		}
		
		LOG.info("recover CardinalityContainer, file:"+file);
		ObjectInputStream ois = null;
		try{
			Map result = new ConcurrentHashMap();
			ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream(file)));
			ICardinalityWraper w = null;
			try {
				while(true) {
					w = (ICardinalityWraper)ois.readObject();
					result.put(w.key, HyperLogLog.Builder.build(w.bytes));
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
	
	public static class ICardinalityWraper implements Serializable{
		
		private static final long serialVersionUID = -6078205760962257019L;
		String key;
		byte[] bytes;
		public ICardinalityWraper(String key, ICardinality c) throws IOException {
			super();
			this.key = key;
			this.bytes = c.getBytes();
		}
	}
}
