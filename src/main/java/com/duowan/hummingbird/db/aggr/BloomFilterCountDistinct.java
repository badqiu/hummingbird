package com.duowan.hummingbird.db.aggr;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
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

/**
 * 描述：返回各分组按字段排序后 （第一or最后）相应的同一记录的若干字段
 * 
 * 参数说明：
 *      col0 String (必填) distinct column 如passport ，mid 等需要去重的列 
 *      col1 String (必填) bloomfilterDb如：default
 *      col2 String (必填) bloomfilterGroup 如：history
 *      col3 String (必填) bloomfilter partition 如：hash(game)、game、extract(parse('20141121', 'yyyyMMdd'), 'yyyyMMdd') 等  列需要在group by中
 *     
 * 函数原型：  Integer bf_count_distinct(String col0,String col1,String col2[, primitive col3, ...])
 * 
 * 返回值：
{distinct_count=2, game_server=s2, sum_dur=8.0, game=as}
{distinct_count=1, game_server=s6, sum_dur=5.0, game=hz}
{distinct_count=2, game_server=s3, sum_dur=10.0, game=hz}
{distinct_count=2, game_server=s1, sum_dur=6.0, game=ddt}
{distinct_count=1, game_server=s5, sum_dur=4.0, game=as}
{distinct_count=2, game_server=s4, sum_dur=12.0, game=ddt}
 * 使用例子：
 	    select game,game_server, sum(dur),bf_count_distinct(passport,'test_db_01','minute',format(parse('20141121','yyyyMMdd'),'yyyyMMdd')) as distinct_count from user   group by extract(parse('20141121','yyyyMMdd'),'yyyyMMdd'),game,game_server
 * 
 * @author Administrator
 *
 */
public class BloomFilterCountDistinct extends BaseCountDistinct{

	private BloomFilterDB db = new BloomFilterDB("/data2/abc");
	
	public BloomFilterCountDistinct() {
	}
	
	public int distinctByHistory(List groupBy,Collection localDistinctedValues,Object[] params) {
		Assert.notEmpty(params,"bloomFilterName must be not empty");
		String bloomFilterName = (String)params[0];
		Assert.hasText(bloomFilterName,"bloomFilterName must be not empty");
		
		String group = StringUtils.join(groupBy,"/");
		
		Date groupDateValue = findDateValue(groupBy);
		
		String partition = "" + new DateConvertUtils().format(groupDateValue, "yyyyMMdd");
		
		BloomFilter bf = db.get(bloomFilterName, partition);
		return bf.notContainsCountAndAdd(group,localDistinctedValues);
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
	public Map<GroupByValue, Object> execByBatch(Map<GroupByValue, List<Map>> map,Object[] params) {
		if(ObjectUtils.isEmpty(params)  || params.length<4) {
			throw new RuntimeException("miss aggr params error,usage bf_count_distinct(<distinctKey> ,<bfDb> ,<fbGroup> ,<bfPartition> )");
		}
		// 参数，支持表达式,不修改原来的实现逻辑
		String distinctColumn = (String) params[0];
		String bloomfilterDbColumn =(String)params[1] ; 
		String bloomfilterGroupColumn =(String)params[2] ; 
		String partitionColumn = (String) params[3];
		
		// group 支持方法获取，但不从数据中获取
		String bloomfilterDb= (String)MVELUtil.eval( bloomfilterDbColumn, new HashMap());
		String bloomfilterGroup = (String)MVELUtil.eval( bloomfilterGroupColumn, new HashMap());
		
		
		List<DistinctRequest> bfGroupQuery = toDistinctRequestList(partitionColumn, distinctColumn, map, params);
		Map<String,Integer> resultMap = AggrFunctionRegister.getInstance().getCountDistinctProvider().bloomFilterNotContainsCountAndAdd(bloomfilterDb,bloomfilterGroup, bfGroupQuery );
		return mapping2Result(map, resultMap);
	}
	
	private static List<DistinctRequest> toDistinctRequestList(String partitionColumn,String distinctColumn,Map<GroupByValue, List<Map>> funcParam,Object[] params) {
		List<DistinctRequest> drList = new ArrayList<DistinctRequest>(funcParam.size());
		for(Map.Entry<GroupByValue, List<Map>> entry : funcParam.entrySet()) {
			GroupByValue key = entry.getKey();
			List<Map> values = entry.getValue();
			String group = StringUtils.join(key.list,"/");
			
			DistinctRequest dr = new DistinctRequest();
			dr.setGroup(group);
			
			List<DistinctData> distinctValueWithExt = new ArrayList<DistinctData>() ;
			for(Map map:values){
				// 第一个不为null的值
				if(StringUtils.isBlank(dr.getPartition())){
					String partition =  String.valueOf(MVELUtil.eval(partitionColumn,map));
					dr.setPartition(partition);
				}
				Object tmpDistinct = MVELUtil.eval(distinctColumn,map);
				if(tmpDistinct == null){
					continue ;
				}
				String distinctValue = String.valueOf( tmpDistinct);
				DistinctData dd = new DistinctData(distinctValue);
				dd.setExt( new HashMap<String, String>()) ;
				
				distinctValueWithExt.add(dd) ;
			}
			
			dr.setDistinctDatas(distinctValueWithExt );
			
			drList.add(dr);
		}
		return drList;
	}

//	private static Map<GroupByValue, Object> mapping2Result(Map<GroupByValue, List<Map>> funcParam, Map<String,Integer> distinctResultMap) {
//		Map<GroupByValue,Object> result = new HashMap<GroupByValue,Object>();
//		
//		for(Map.Entry<GroupByValue, List<Map>> entry : funcParam.entrySet()) {
//			GroupByValue key = entry.getKey();
//			String group = StringUtils.join(key.list,"/");
//			List<DistinctData> distinctResult = distinctResultMap.get(group);
//			if(distinctResult == null) {
//				throw new RuntimeException("not found result for group:"+group);
//			}
//			Object aggrResult = packAggrResult(distinctResult) ;
//			result.put(key, aggrResult);
//		}
//		return result;
//	}
	
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
