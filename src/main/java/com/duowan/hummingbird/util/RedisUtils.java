package com.duowan.hummingbird.util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mvel2.MVEL;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

import com.github.rapid.common.redis.RedisTemplate;
import com.github.rapid.common.redis.RedisTransactionCallback;

public class RedisUtils {

//	public void batchRedis(JedisPool jedisPool,final List<Map> datas, final String expression) throws Exception {
//		RedisTemplate template = new RedisTemplate(jedisPool);
//		
//		final Serializable expr = MVEL.compileExpression(expression);
//		template.execute(new RedisTransactionCallback(){
//			@Override
//			public Object doInTransaction(Transaction redis) {
//				for(Map row :  datas) {
//					Map vars = new HashMap();
//					vars.put("redis", redis);
//					vars.put("row",row);
//					MVEL.executeExpression(expr,vars);
//				}
//				
//				redis.exec();
//				return null;
//			}
//		});
//	}
	
	
	public static void batchRedis(JedisPool jedisPool,final List<Map> datas, final String expression) throws Exception {
		RedisTemplate template = new RedisTemplate(jedisPool);
		String newExpr = String.format("foreach(row : datas) { var redis = redis; %s}",expression);
		final Serializable expr = MVEL.compileExpression(newExpr);
		template.execute(new RedisTransactionCallback(){
			@Override
			public Object doInTransaction(Transaction redis) {
				Map vars = new HashMap();
				vars.put("datas", datas);
				vars.put("redis", redis);
				MVELUtil.executeExpression(expr,vars);
				redis.exec();
				return null;
			}
		});
	}
	
}
