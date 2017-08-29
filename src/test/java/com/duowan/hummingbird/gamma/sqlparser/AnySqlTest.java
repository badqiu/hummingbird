package com.duowan.hummingbird.gamma.sqlparser;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import net.sf.jsqlparser.JSQLParserException;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import com.github.rapid.common.io.freemarker.FreemarkerReader;

import freemarker.template.Configuration;

public class AnySqlTest {

	@Test
	public void testUse() throws JSQLParserException, IOException {
		System.out.println(new ArrayList().subList(0, 6));
		System.out.println(Arrays.asList(1,5.20,"badqiu"));
		System.out.println(System.currentTimeMillis());
		Map param = new HashMap();
		param.put("day", "2017-10-10");
		FreemarkerReader reader = new FreemarkerReader(new StringReader("<#if day?replace('-','')?number &gt; 2016 >true,${day?replace('-','')?number}</#if>"),new Configuration(),param);
		System.out.println(IOUtils.toString(reader));
//		System.out.println("中".length());
//		printABCD(6);
//		System.out.println(new Timestamp(1476806400000L));
//		System.out.println(System.getenv("DWENV"));
//		
//		System.out.println("01:"+('\001' == '\1'));
//		
//		System.out.println(new Timestamp(1468252800000L));
//		System.out.println(System.getProperty("user.home"));
//		System.out.println(new Timestamp(1451921460000l));
//		
//		System.out.println((Integer.MAX_VALUE / 1024 / 1024) + "mb");
//		CCJSqlParserManager pm = new CCJSqlParserManager();
//		net.sf.jsqlparser.statement.Statement stmt = pm.parse(new StringReader("use abc"));
//		System.out.println("stmt:"+stmt);
	}

	private void printABCD(int sum) {
		double a = sum * 0.2;
		double b = sum * 0.3;
		double c = sum * 0.4;
		double d = sum * 0.1;
		System.out.println("A:"+a+"\nB:"+b+"\nC:"+c+"\nD:"+d+"\n\n\n");
	}
}
