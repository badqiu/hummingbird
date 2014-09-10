package com.duowan.hummingbird;

import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;

import redis.clients.jedis.JedisPool;

import com.duowan.hummingbird.db.BirdDatabase;
import com.duowan.hummingbird.util.RedisUtils;
import com.duowan.hummingbird.util.SpringJdbcUtil;

public class BirdQueryExecutor {
	private BirdDatabase birdDatabase;
	private JedisPool jedisPool;
	private DataSource dataSource;

	private String query;
	private String intoRedis;
	private String intoDb;

	public void exec() throws Exception {
		List<Map> rows = birdDatabase.select(query);
		if (StringUtils.isNotBlank(intoRedis)) {
			RedisUtils.batchRedis(jedisPool, rows, intoRedis);
		}
		if (StringUtils.isNotBlank(intoDb)) {
			SpringJdbcUtil.batchUpdate(dataSource, intoDb, rows);
		}
	}
	
	public BirdDatabase getBirdDatabase() {
		return birdDatabase;
	}

	public void setBirdDatabase(BirdDatabase birdDatabase) {
		this.birdDatabase = birdDatabase;
	}

	public JedisPool getJedisPool() {
		return jedisPool;
	}

	public void setJedisPool(JedisPool jedisPool) {
		this.jedisPool = jedisPool;
	}

	public DataSource getDataSource() {
		return dataSource;
	}

	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public String getIntoRedis() {
		return intoRedis;
	}

	public void setIntoRedis(String intoRedis) {
		this.intoRedis = intoRedis;
	}

	public String getIntoDb() {
		return intoDb;
	}

	public void setIntoDb(String intoDb) {
		this.intoDb = intoDb;
	}


}
