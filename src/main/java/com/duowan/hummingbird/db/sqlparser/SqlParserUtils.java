package com.duowan.hummingbird.db.sqlparser;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.WithItem;

public class SqlParserUtils {

	public static List<String> parseForGroupByItems(String groupBy) {
		try {
			String query = "select * from t group by "+groupBy;
			CCJSqlParserManager pm = new CCJSqlParserManager();
			net.sf.jsqlparser.statement.Statement stmt =pm.parse(new StringReader(query));
			if(stmt instanceof Select) {
				Select s = (Select)stmt;
				final List<String> groupByList = new ArrayList();
				s.getSelectBody().accept(new SelectVisitor() {
					@Override
					public void visit(WithItem withItem) {
					}
					@Override
					public void visit(SetOperationList setOpList) {
					}
					
					@Override
					public void visit(PlainSelect plainSelect) {
						for(Expression expr : plainSelect.getGroupByColumnReferences() ){
							groupByList.add(expr.toString());
						}
					}
				});
				return groupByList;
			}
		}catch(Exception e) {
			throw new RuntimeException("error group by statement:"+groupBy,e);
		}
		throw new RuntimeException("error group by statement:"+groupBy);
	}
}
