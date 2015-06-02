package com.duowan.hummingbird.gamma.sqlparser;

import java.io.StringReader;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;

import org.junit.Test;

public class AnySqlTest {

	@Test
	public void testUse() throws JSQLParserException {
		CCJSqlParserManager pm = new CCJSqlParserManager();
		net.sf.jsqlparser.statement.Statement stmt = pm.parse(new StringReader("use abc"));
		System.out.println("stmt:"+stmt);
	}
}