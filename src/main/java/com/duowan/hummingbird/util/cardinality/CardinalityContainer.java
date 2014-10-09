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

import com.clearspring.analytics.stream.cardinality.ICardinality;

public class CardinalityContainer {
	private final static Logger LOG = LoggerFactory.getLogger(PartitionedCardinalityContainer.class);
	
	private Map<String, ICardinality> container = new ConcurrentHashMap<String, ICardinality>();
	private String baseDir;
	private boolean changed = false;
	private CardinalityFactory factory;
	public CardinalityContainer(String baseDir)  {
		Assert.hasText(baseDir,"baseDir must be not empty");
		this.baseDir = baseDir;
	}
	
	public Map<String, ICardinality> getPartitons(){
		return container;
	}

	public ICardinality getCardinality(String key) {
		changed = true;
		ICardinality r = container.get(key);
		if(r == null) {
			r = factory.create();
			container.put(key, r);
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
			for(Map.Entry<String, ICardinality> entry : container.entrySet()) {
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
		container = recover0();
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
					result.put(w.key, factory.recover(w.bytes));
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
