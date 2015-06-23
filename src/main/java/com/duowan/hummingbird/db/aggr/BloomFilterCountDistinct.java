package com.duowan.hummingbird.db.aggr;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.duowan.hummingbird.db.sql.select.AggrFunctionRegister;
import com.duowan.hummingbird.db.sql.select.SelectSql.GroupByValue;
import com.duowan.hummingbird.util.MVELUtil;
import com.duowan.hummingbird.util.bloomfilter.BloomFilter;
import com.duowan.hummingbird.util.bloomfilter.BloomFilterDB;
import com.github.distinct_server.api.BloomFilterRequest;
import com.github.rapid.common.util.DateConvertUtil;

/**
 * 描述：返回各分组按字段排序后 （第一or最后）相应的同一记录的若干字段
 * 
 * 参数说明：
 *      distinct_column (必填) distinct column 如passport ，mid 等需要去重的列 
 *      bloomfilterName (必填) bloomfilterName 如：history
 *      bloomfilterPartition (必填) bloomfilter partition 如：hash(game)、game、extract(parse('20141121', 'yyyyMMdd'), 'yyyyMMdd') 等  列需要在group by中
 * 函数  Integer bf_count_distinct(String distinct_column,String bloomfilterName,bloomfilterPartition)
 * 
 * 返回值：
{distinct_count=2, game_server=s2, sum_dur=8.0, game=as}
{distinct_count=1, game_server=s6, sum_dur=5.0, game=hz}
{distinct_count=2, game_server=s3, sum_dur=10.0, game=hz}
{distinct_count=2, game_server=s1, sum_dur=6.0, game=ddt}
{distinct_count=1, game_server=s5, sum_dur=4.0, game=as}
{distinct_count=2, game_server=s4, sum_dur=12.0, game=ddt}
 * 使用例子：
 	    select 
 	    	game,game_server, sum(dur),
 	    	bf_count_distinct(passport,'minute_bf',format(parse('20141121','yyyyMMdd'),'yyyyMMdd')) as distinct_count 
 	    from user   
 	    group by extract(parse('20141121','yyyyMMdd'),'yyyyMMdd'),game,game_server
 * 
 * @author badqiu
 *
 */
public class BloomFilterCountDistinct extends BaseCountDistinct{

//	private BloomFilterDB db = new BloomFilterDB("/data2/bloomfilter/local_cache");
	
	public BloomFilterCountDistinct() {
	}
	
	@Override
	public int distinctByHistory(List groupBy,Collection localDistinctedValues,Object[] params) {
//		Assert.notEmpty(params,"bloomFilterName must be not empty");
//		String bloomFilterName = (String)params[0];
//		Assert.hasText(bloomFilterName,"bloomFilterName must be not empty");
//		
//		String group = StringUtils.join(groupBy,"/");
//		
//		Date groupDateValue = findDateValue(groupBy);
//		
//		String partition = "" + DateConvertUtil.format(groupDateValue, "yyyyMMdd");
//		
//		BloomFilter bf = db.get(bloomFilterName, partition);
//		return bf.notContainsCountAndAdd(group,localDistinctedValues);
		throw new UnsupportedOperationException("not yet implement");
	}

	private static Date findDateValue(List groupBy) {
		for(Object g : groupBy) {
			if(g instanceof Date) {
				return (Date)g;
			}
		}
		return null;
	}
	
	@Override
	public Map<GroupByValue, Object> execByBatch(Map<GroupByValue, List<Map>> groupByParam,Object[] params) {
		if(ObjectUtils.isEmpty(params)  || params.length < 3) {
			throw new RuntimeException("miss aggr params error,usage bf_count_distinct(<distinctKey> ,<bfName> ,<bfPartition> )");
		}
		// 参数，支持表达式,不修改原来的实现逻辑
		String distinctColumnExpr = (String) params[0];
		String bloomfilterGroupExpr =(String)params[1] ; 
		String partitionExpr = (String) params[2];
		
		// group 支持方法获取，但不从数据中获取
		String bloomfilterName = (String)MVELUtil.eval( bloomfilterGroupExpr, new HashMap());
		
		return execByBatch(groupByParam, distinctColumnExpr, partitionExpr, bloomfilterName);
	}

	private Map<GroupByValue, Object> execByBatch(
			Map<GroupByValue, List<Map>> groupByParam,
			String distinctColumnExpr, String partitionExpr,
			String bloomfilterName) {
		List<BloomFilterRequest> bfGroupQuery = toBloomFilterRequest(partitionExpr, distinctColumnExpr, groupByParam);
		Map<String,Integer> resultMap = AggrFunctionRegister.getInstance().getCountDistinctProvider().bloomFilterNotContainsCountAndAdd(bloomfilterName, bfGroupQuery );
		return mapping2Result(groupByParam, resultMap);
	}
	
	private static List<BloomFilterRequest> toBloomFilterRequest(String partitionColumn,String distinctColumn,Map<GroupByValue, List<Map>> groupByParam) {
		List<BloomFilterRequest> resultList = new ArrayList<BloomFilterRequest>(groupByParam.size());
		for(Map.Entry<GroupByValue, List<Map>> entry : groupByParam.entrySet()) {
			GroupByValue key = entry.getKey();
			List<Map> values = entry.getValue();
			String group = StringUtils.join(key.list,"/");
			BloomFilterRequest request = newBloomFilterRequest(partitionColumn,distinctColumn, values, group);
			resultList.add(request);
		}
		return resultList;
	}

	private static BloomFilterRequest newBloomFilterRequest(
			String partitionColumn, String distinctColumn, List<Map> values,
			String group) {
		BloomFilterRequest request = new BloomFilterRequest();
		
		String partition = getBloomFilterPartition(partitionColumn, values, request);
		
		request.setBloomfilterPartition(partition);
		request.setGroup(group);
		request.setKeys(new HashSet(getDistinctValues(distinctColumn, values)));
		return request;
	}

	private static List<String> getDistinctValues(String distinctColumn,
			List<Map> values) {
		List<String> distinctValues = new ArrayList();
		for(Map map : values){
			Object distinctValue = MVELUtil.eval(distinctColumn,map);
			if(distinctValue == null){
				continue ;
			}
			distinctValues.add(String.valueOf(distinctValue));
		}
		return distinctValues;
	}

	private static String  getBloomFilterPartition(String partitionExpr,
			List<Map> values, BloomFilterRequest request) {
		for(Map map : values){
			// 第一个不为null的值
			if(StringUtils.isBlank(request.getBloomfilterPartition())){
				String partition =  String.valueOf(MVELUtil.eval(partitionExpr,map));
				return partition;
			}
		}
		return null;
	}
	
	public static Map<GroupByValue, Object> mapping2Result(Map<GroupByValue, List<Map>> map, Map<String, Integer> resultMap) {
		Map<GroupByValue,Object> result = new HashMap<GroupByValue,Object>();
		for(Map.Entry<GroupByValue, List<Map>> entry : map.entrySet()) {
			GroupByValue key = entry.getKey();
			String group = StringUtils.join(key.list,"/");
			Object aggrResult = resultMap.get(group);
			if(aggrResult == null) {
				throw new RuntimeException("not found result for group:"+group);
			}
			result.put(key, aggrResult);
		}
		return result;
	}


	
}
