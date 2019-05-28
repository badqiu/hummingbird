package com.duowan.hummingbird.gamma.sqlparser;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.JSQLParserException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import com.github.rapid.common.io.freemarker.FreemarkerReader;

import freemarker.template.Configuration;

public class AnySqlTest {

	@Test
	public void testUse() throws JSQLParserException, IOException {
		List<String> line1 = FileUtils.readLines(new File("H:\\tmp\\Mi-data\\1/fact_create.csv"));
		List<String> line2 = FileUtils.readLines(new File("H:\\tmp\\Mi-data\\2/fact_create.csv"));
		line2.remove(0);
		line1.addAll(line2);
		FileUtils.writeLines(new File("H:\\tmp\\Mi-data\\create.csv"), line1);
		
		System.out.println(new Timestamp(1509246868000L));
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


}
