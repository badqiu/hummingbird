package com.duowan.hummingbird.db.aggr;

import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.ComparatorUtils;
import org.apache.commons.collections.comparators.ComparableComparator;
import org.apache.commons.lang.StringUtils;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.duowan.hummingbird.util.MVELUtil;

/**
 * 
 * 
* 描述：返回各分组按字段排序后 （第一or最后）相应的同一记录的若干字段
 * 
 * 参数说明：
 *      col1 String (必填) order [asc,desc] 不区分大小写 默认为asc; 
 *      col2 T (必填) column for order , first input(output) column  供排序列 ;可排序的基本数据类型，不支持map、list、Struct 等复杂对象 
 *      col ... T ...  其他需要返回带上的列  所有数据类型均支持;  
 *     
 * 函数原型： Map<GroupByValue, Object> order_first_row(String col1,comparable_primitive col2[, primitive col3, ...])
 * 
 * 返回值：
 * 			{ext_columns={id=2, 'desc'=desc, dur=2, stime=1999-01-03 00:00:00.0, game=hz, passport=2, ext={ext_key=ext_value_2, ext_num=2}}, game_server=s3}
 * 			{ext_columns={id=7, 'desc'=desc, dur=7, stime=1999-01-04 00:00:00.0, game=as, passport=7, ext={ext_key=ext_value_1, ext_num=7, ext_7=1}}, game_server=s2}
 * 			{ext_columns={id=6, 'desc'=desc, dur=6, stime=1999-01-03 00:00:00.0, game=ddt, passport=6, ext={ext_key=ext_value_0, ext_num=6, ext_6=0}}, game_server=s1}
 * 			{ext_columns={id=3, 'desc'=desc, dur=3, stime=1999-01-04 00:00:00.0, game=ddt, passport=3, ext={ext_3=1, ext_key=ext_value_0, ext_num=3}}, game_server=s4}
 * 			{ext_columns={id=4, 'desc'=desc, dur=4, stime=1999-01-01 00:00:00.0, game=as, passport=4, ext={ext_key=ext_value_1, ext_num=4}}, game_server=s5}
 *
 * 使用例子：
 	    select game_server,order_first_row('desc',stime,passport,game,id,dur,ext) AS ext_columns from user group by game_server
 * 
 * @author luowen
 *
 */
public class OrderFirstRow extends BaseAggrFunction implements AggrFunction {

	private static final String DESC  = "desc" ;
	private static final String ASC  = "asc" ;
	
	public OrderFirstRow() {
	}

	@Override
	public Object exec(List groupBy, List<Map> values, Object[] params) {
		if (ObjectUtils.isEmpty(params) || params.length<2) {
			throw new IllegalArgumentException("at least two aggr params for OrderFirstRow function!");
		}
		return getFirstValue(values,params);
	}
	
	private Map getFirstValue(List<Map> value, Object[] params) {
		final String orderStrategyColumn = String.valueOf(params[0]);
		final String orderColumn = String.valueOf(params[1]);
		Collections.sort(value,new Comparator<Map>() {
			@Override
			public int compare(Map arg0, Map arg1) {
				String order = (String)MVELUtil.eval(orderStrategyColumn, arg0) ;
				Object orderValue0 = MVELUtil.eval(orderColumn, arg0);
				Object orderValue1 = MVELUtil.eval(orderColumn, arg1);
				if(StringUtils.equalsIgnoreCase(ASC, order)){
					return compareObject(orderValue0,orderValue1);
				}
				Assert.isTrue(StringUtils.equalsIgnoreCase(DESC, order), "orderStrategy should be asc or desc and ignore case. ");
				return compareObject(orderValue1,orderValue0);
			}

			private int compareObject(Object orderValue0, Object orderValue1) {
				return ComparableComparator.getInstance().compare(orderValue0, orderValue1) ;
			}
			
		});
		// do some check !
		Map firstValue = value.get(0) ;
		Map result = new HashMap();
		for(Object p : params){
			result.put(p, MVELUtil.eval(String.valueOf(p), firstValue)) ;
		}
		return result;
	}



}
