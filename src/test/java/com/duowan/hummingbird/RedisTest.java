package com.duowan.hummingbird;

import static junit.framework.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.mvel2.MVEL;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import com.duowan.common.redis.JedisPoolFactoryBean;
import com.duowan.common.redis.RedisTemplate;
import com.duowan.common.util.Profiler;
import com.duowan.hummingbird.util.RedisUtils;

public class RedisTest {

//	RedisServer redisServer = null;
	JedisPool jedisPool = null;
	@Before
	public void bofore() throws Exception {
		jedisPool = newJedisPool();
	}
	private JedisPool newJedisPool() throws Exception {
		JedisPoolConfig poolConfig = new JedisPoolConfig();
		JedisPoolFactoryBean factoryBean = new JedisPoolFactoryBean();
		factoryBean.setServer("localhost:6379");
		factoryBean.setPoolConfig(poolConfig);
		factoryBean.afterPropertiesSet();
		JedisPool jedisPool = factoryBean.getObject();
		return jedisPool;
	}
//	@After() 
//	public void after() throws InterruptedException {
//		redisServer.stop();
//	}
	
	@Test
	public void test() throws Exception {
		MVEL.compileExpression("with(row) {redis = '123', sex=1}");
		assertTrue(true);
		final List<Map> datas = new ArrayList<Map>();
		datas.addAll(Arrays.asList(TestData.getTestDatas(200000)));

		final String expression = "var key = row.game + '/' +  row.game_server + '/dur'; redis.set(key,row.dur); redis.expire(key,3);";
		Profiler.start("batch1",datas.size());
		RedisUtils.batchRedis(jedisPool,datas, expression);
		Profiler.release();
		System.out.println(Profiler.dump());
		
		Profiler.start("batch2",datas.size());
		RedisUtils.batchRedis(jedisPool,datas, expression);
		Profiler.release();
		
		System.out.println(Profiler.dump());
		
		RedisTemplate t = new RedisTemplate(jedisPool);
		Assert.assertEquals( t.get("ddt/s1/dur"), "998");
		Thread.sleep(1000 * 3);
		Assert.assertEquals( t.get("ddt/s1/dur"), null);
	}


	
}
